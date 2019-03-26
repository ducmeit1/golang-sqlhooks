package sqlhooks

import (
	"context"
	"database/sql/driver"
	"errors"
)

type Hook func(ctx context.Context, query string, args ...interface{}) (context.Context, error)
type ErrorHook func(ctx context.Context, err error, query string, args ...interface{}) error

type Hooks interface {
	Before(ctx context.Context, query string, args ...interface{}) (context.Context, error)
	After(ctx context.Context, query string, args ...interface{}) (context.Context, error)
}

type HookErrors interface {
	OnError(ctx context.Context, err error, query string, args ...interface{}) error
}

type Driver struct {
	driver.Driver
	Hooks
}

func (drv *Driver) Open(name string) (driver.Conn, error) {
	conn, err := drv.Driver.Open(name)
	if err != nil {
		return conn, err
	}

	wrapped := &Conn{conn, drv.Hooks}
	iE, iQ := isExecer(conn), isQueryer(conn)
	if iE && iQ {
		return &ExecerQueryerContext{wrapped, &ExecerContext{wrapped}, &QueryerContext{wrapped}}, nil
	}
	if iE {
		return &ExecerContext{wrapped}, nil
	}
	if iQ {
		return &QueryerContext{wrapped}, nil
	}
	return wrapped, nil
}

type Conn struct {
	driver.Conn
	Hooks
}

func (conn *Conn) Prepare(query string) (driver.Stmt, error) { return conn.Conn.Prepare(query) }
func (conn *Conn) Close() error                              { return conn.Conn.Close() }
func (conn *Conn) Begin() (driver.Tx, error)                 { return conn.Conn.Begin() }
func (conn *Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if ciCtx, is := conn.Conn.(driver.ConnBeginTx); is {
		return ciCtx.BeginTx(ctx, opts)
	}
	return nil, errors.New("[sql] driver does not support non-default isolation level")
}

func (conn *Conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	var (
		stmt driver.Stmt
		err  error
	)

	if c, ok := conn.Conn.(driver.ConnPrepareContext); ok {
		stmt, err = c.PrepareContext(ctx, query)
	} else {
		stmt, err = conn.Prepare(query)
	}

	if err != nil {
		return stmt, err
	}

	return &Stmt{stmt, conn.Hooks, query}, nil
}

func (conn *Conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	var err error
	cArgs := namedValueToInterface(args)

	if ctx, err = conn.Hooks.Before(ctx, query, cArgs...); err != nil {
		return nil, err
	}

	result, err := conn.execContext(ctx, query, args)
	if err != nil {
		return result, handlerError(ctx, conn.Hooks, err, query, cArgs...)
	}

	if ctx, err = conn.Hooks.After(ctx, query, cArgs...); err != nil {
		return nil, err
	}

	return result, err
}

func (conn *Conn) execContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	switch c := conn.Conn.(type) {
	case driver.ExecerContext:
		return c.ExecContext(ctx, query, args)
	case driver.Execer:
		cArgs, err := namedValueToValue(args)
		if err != nil {
			return nil, err
		}
		return c.Exec(query, cArgs)
	default:
		return nil, errors.New("[sql-hook] ExecContext has created, but something went wrong")
	}
}

type ExecerContext struct{ *Conn }

func (ec *ExecerContext) execContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	switch c := ec.Conn.Conn.(type) {
	case driver.ExecerContext:
		return c.ExecContext(ctx, query, args)
	case driver.Execer:
		cArgs, err := namedValueToValue(args)
		if err != nil {
			return nil, err
		}
		return c.Exec(query, cArgs)
	default:
		return nil, errors.New("[sql-hook] ExecContext has created, but something went wrong")
	}
}

func (ec *ExecerContext) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	var err error
	cArgs := namedValueToInterface(args)

	if ctx, err = ec.Hooks.Before(ctx, query, cArgs...); err != nil {
		return nil, err
	}

	results, err := ec.execContext(ctx, query, args)
	if err != nil {
		return results, handlerError(ctx, ec.Hooks, err, query, cArgs...)
	}

	if ctx, err = ec.Hooks.After(ctx, query, cArgs...); err != nil {
		return nil, err
	}

	return results, err
}

type QueryerContext struct{ *Conn }

func (qc *QueryerContext) queryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	switch c := qc.Conn.Conn.(type) {
	case driver.QueryerContext:
		return c.QueryContext(ctx, query, args)
	case driver.Queryer:
		cArgs, err := namedValueToValue(args)
		if err != nil {
			return nil, err
		}
		return c.Query(query, cArgs)
	default:
		return nil, errors.New("[sql-hook] QueryContext has created, but something went to wrong")
	}
}

func (qc *QueryerContext) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	var err error
	cArgs := namedValueToInterface(args)

	if ctx, err = qc.Hooks.Before(ctx, query, cArgs...); err != nil {
		return nil, err
	}

	results, err := qc.queryContext(ctx, query, args)
	if err != nil {
		return results, handlerError(ctx, qc.Hooks, err, query, cArgs...)
	}

	if ctx, err = qc.Hooks.After(ctx, query, cArgs...); err != nil {
		return nil, err
	}

	return results, err
}

type ExecerQueryerContext struct {
	*Conn
	*ExecerContext
	*QueryerContext
}
type Stmt struct {
	driver.Stmt
	Hooks
	query string
}

func (stmt *Stmt) Close() error                                    { return stmt.Stmt.Close() }
func (stmt *Stmt) NumInput() int                                   { return stmt.Stmt.NumInput() }
func (stmt *Stmt) Exec(args []driver.Value) (driver.Result, error) { return stmt.Stmt.Exec(args) }
func (stmt *Stmt) Query(args []driver.Value) (driver.Rows, error)  { return stmt.Stmt.Query(args) }

func (stmt *Stmt) execContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if s, ok := stmt.Stmt.(driver.StmtExecContext); ok {
		return s.ExecContext(ctx, args)
	}

	values := make([]driver.Value, len(args))
	for _, arg := range args {
		values[arg.Ordinal-1] = arg.Value
	}

	return stmt.Exec(values)
}

func (stmt *Stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	var err error
	cArgs := namedValueToInterface(args)

	if ctx, err = stmt.Hooks.Before(ctx, stmt.query, cArgs...); err != nil {
		return nil, err
	}

	results, err := stmt.execContext(ctx, args)

	if err != nil {
		return results, handlerError(ctx, stmt.Hooks, err, stmt.query, cArgs...)
	}

	if ctx, err = stmt.Hooks.After(ctx, stmt.query, cArgs...); err != nil {
		return nil, err
	}

	return results, err
}

func (stmt *Stmt) queryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if s, ok := stmt.Stmt.(driver.StmtQueryContext); ok {
		return s.QueryContext(ctx, args)
	}

	values := make([]driver.Value, len(args))
	for _, arg := range args {
		values[arg.Ordinal-1] = arg.Value
	}
	return stmt.Query(values)
}

func (stmt *Stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	var err error
	cArgs := namedValueToInterface(args)

	if ctx, err = stmt.Hooks.Before(ctx, stmt.query, cArgs...); err != nil {
		return nil, err
	}

	rows, err := stmt.queryContext(ctx, args)
	if err != nil {
		return rows, handlerError(ctx, stmt.Hooks, err, stmt.query, cArgs...)
	}

	if ctx, err = stmt.Hooks.After(ctx, stmt.query, cArgs...); err != nil {
		return nil, err
	}

	return rows, err
}

func Wrap(drv driver.Driver, hooks Hooks) driver.Driver {
	return &Driver{drv, hooks}
}

func isExecer(conn driver.Conn) bool {
	switch conn.(type) {
	case driver.ExecerContext:
		return true
	case driver.Execer:
		return true
	default:
		return false
	}
}

func isQueryer(conn driver.Conn) bool {
	switch conn.(type) {
	case driver.QueryerContext:
		return true
	case driver.Queryer:
		return true
	default:
		return false
	}
}

func handlerError(ctx context.Context, hooks Hooks, err error, query string, args ...interface{}) error {
	h, ok := hooks.(HookErrors)
	if !ok {
		return err
	}

	if err := h.OnError(ctx, err, query, args...); err != nil {
		return err
	}

	return err
}

func namedValueToValue(named []driver.NamedValue) ([]driver.Value, error) {
	args := make([]driver.Value, len(named))
	for i, n := range named {
		if len(n.Name) > 0 {
			return nil, errors.New("[sql]: driver does not support the use of Named Parameter")
		}
		args[i] = n.Value
	}
	return args, nil
}

func namedValueToInterface(named []driver.NamedValue) []interface{} {
	itf := make([]interface{}, len(named))
	for i, n := range named {
		itf[i] = n
	}
	return itf
}
