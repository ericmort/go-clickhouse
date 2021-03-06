package clickhouse

import (
	"errors"
	"fmt"
	"strings"
)

type (
	Column  string
	Columns []string
	Row     []interface{}
	Rows    []Row
	Array   []interface{}
)

func NewHttpTransport() *HttpTransport {
	return &HttpTransport{}
}

func NewConn(host string, t *HttpTransport) *Conn {
	host = "http://" + strings.Replace(host, "http://", "", 1)
	host = strings.TrimRight(host, "/") + "/"

	return &Conn{
		Host:      host,
		transport: t,
	}
}

func NewQuery(stmt string, args ...interface{}) Query {
	return Query{
		Stmt: stmt,
		args: args,
	}
}

func BuildInsert(tbl string, cols Columns, row Row) (Query, error) {
	return BuildMultiInsert(tbl, cols, Rows{row})
}

func BuildMultiInsert(tbl string, cols Columns, rows Rows) (Query, error) {
	var (
		stmt string
		args []interface{}
	)

	colCount := len(cols)
	rowCount := len(rows)
	args = make([]interface{}, colCount*rowCount)
	argi := 0
	for _, row := range rows {
		if len(row) != colCount {
			return Query{}, errors.New("Amount of row items does not match column count")
		}
		for _, val := range row {
			args[argi] = val
			argi++
		}
	}

	binds := strings.Repeat("?,", colCount)
	binds = "(" + binds[:len(binds)-1] + "),"
	batch := strings.Repeat(binds, rowCount)
	batch = batch[:len(batch)-1]

	stmt = fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", tbl, strings.Join(cols, ","), batch)

	return NewQuery(stmt, args...), nil
}
