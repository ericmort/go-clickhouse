package clickhouse

import (
	"errors"
	"strings"
)

type Query struct {
	Stmt string
	args []interface{}
}

func (q Query) Iter(conn *Conn) *Iter {
	if conn == nil {
		return &Iter{err: errors.New("Connection pointer is nil")}
	}
	resp, err := conn.transport.Exec(conn, q, false)
	if err != nil {
		return &Iter{err: err}
	}

	err = errorFromResponse(resp)
	if err != nil {
		return &Iter{err: err}
	}

	return &Iter{text: resp}
}

func (q Query) Exec(conn *Conn) (err error) {
	if conn == nil {
		return errors.New("Connection pointer is nil")
	}
	resp, err := conn.transport.Exec(conn, q, false)
	if err == nil {
		err = errorFromResponse(resp)
	}

	return err
}

type Iter struct {
	colCnt  int64
	err     error
	text    string
	current string
}

func (r *Iter) Error() error {
	return r.err
}

func (r *Iter) Next() bool {
	r.current = r.fetchNext()
	if len(r.current) == 0 {
		return false
	}
	return true
}

func (r *Iter) ColumnCount() int {
	return len(strings.Split(r.current, "\t"))
}

func (r *Iter) Scan(vars ...interface{}) error {
	a := strings.Split(r.current, "\t")
	if len(a) < len(vars) {
		return errors.New("len(a) < len(vars)")
	}
	for i, v := range vars {
		err := unmarshal(v, a[i])
		if err != nil {
			r.err = err
			return err
		}
	}
	return nil
}

func (r *Iter) ScanRow(vars ...interface{}) error {
	return nil
}

func (r *Iter) fetchNext() string {
	var res string
	pos := strings.Index(r.text, "\n")
	if pos == -1 {
		res = r.text
		r.text = ""
	} else {
		res = r.text[:pos]
		r.text = r.text[pos+1:]
	}
	return res
}
