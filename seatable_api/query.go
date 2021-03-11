package seatable_api

import (
	"encoding/json"
	"fmt"
	"github.com/seatable/seatable-api-go/seatable_api/lex"
	"strings"
)

const (
	StringToken lexer.TokenType = iota
	IntegerToken
	OpToken
	QuotaStringToken
	InvalidToken
)

type QuerySet struct {
	Base       *SeaTableAPI
	TableName  string
	RawRows    interface{}
	RawColumns interface{}
	Conditions string
	Rows       interface{}
}

type MyLexer struct {
	*lexer.L
}

func NewQuerySet(base *SeaTableAPI, tableName string) *QuerySet {
	return &QuerySet{Base: base, TableName: tableName}
}

func clone(src interface{}) (*QuerySet, error) {
	var dst = new(QuerySet)
	b, err := json.Marshal(src)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(b, dst)
	return dst, err
}

func (qs *QuerySet) ExecuteConditions() error {
	if qs.Conditions != "" && qs.RawRows != nil && qs.RawColumns != nil {
		parser := NewConditionsParser()
		rows, err := parser.Parse(qs.RawRows, qs.RawColumns, qs.Conditions)
		if err != nil {
			return err
		}
		qs.Rows = rows
		return nil
	}

	qs.Rows = qs.RawRows
	return nil
}

func (qs *QuerySet) Filter(conditions string) (*QuerySet, error) {
	c, err := clone(qs)
	if err != nil {
		return nil, err
	}
	err = c.ExecuteConditions()
	if err != nil {
		return nil, err
	}

	return c, nil
}

func (qs *QuerySet) Get() (interface{}, error) {
	c, err := clone(qs)
	if err != nil {
		return nil, err
	}
	c.ExecuteConditions()
	if err != nil {
		return nil, err
	}

	rows, ok := c.Rows.([]interface{})
	if !ok {
		err := fmt.Errorf("failed to assert rows")
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}

	return rows[0], nil
}

func (qs *QuerySet) All() (*QuerySet, error) {
	c, err := clone(qs)
	if err != nil {
		return nil, err
	}

	return c, nil
}

func (qs *QuerySet) Update(rowData interface{}) (interface{}, error) {
	rows, ok := qs.Rows.([]interface{})
	if !ok {
		err := fmt.Errorf("failed to assert rows")
		return nil, err
	}

	for _, v := range rows {
		row, ok := v.(map[string]interface{})
		if ok {
			rowID, ok := row["_id"].(string)
			if !ok {
				err := fmt.Errorf("failed to assert row id")
				return nil, err
			}
			_, err := qs.Base.UpdateRow(qs.TableName, rowID, rowData)
			if err != nil {
				return nil, err
			}
			data, ok := rowData.(map[string]interface{})
			if ok {
				for k, v := range data {
					row[k] = v
				}
			}
		}
	}

	qs.Rows = rows

	return qs.Rows, nil
}

func (qs *QuerySet) Delete() (int, error) {
	rows, ok := qs.Rows.([]interface{})
	if !ok {
		err := fmt.Errorf("failed to assert rows")
		return 0, err
	}

	var rowIDs []interface{}
	for _, v := range rows {
		row, ok := v.(map[string]interface{})
		if ok {
			rowIDs = append(rowIDs, row["_id"])
		}
	}

	_, err := qs.Base.BatchDeleteRows(qs.TableName, rowIDs)
	if err != nil {
		return 0, err
	}

	return len(rowIDs), nil
}

func (qs *QuerySet) First() (interface{}, error) {
	if qs.Rows == nil {
		return nil, nil
	}
	rows, ok := qs.Rows.([]interface{})
	if !ok {
		err := fmt.Errorf("failed to assert rows")
		return 0, err
	}

	if len(rows) == 0 {
		return nil, nil
	}

	return rows[0], nil
}

func (qs *QuerySet) Last() (interface{}, error) {
	if qs.Rows == nil {
		return nil, nil
	}
	rows, ok := qs.Rows.([]interface{})
	if !ok {
		err := fmt.Errorf("failed to assert rows")
		return 0, err
	}

	if len(rows) == 0 {
		return nil, nil
	}

	return rows[len(rows)-1], nil
}

func (qs *QuerySet) Count() (int, error) {
	if qs.Rows == nil {
		return 0, nil
	}
	rows, ok := qs.Rows.([]interface{})
	if !ok {
		err := fmt.Errorf("failed to assert rows")
		return 0, err
	}

	return len(rows), nil
}

func (qs *QuerySet) Exists() (bool, error) {
	if qs.Rows == nil {
		return false, nil
	}
	rows, ok := qs.Rows.([]interface{})
	if !ok {
		err := fmt.Errorf("failed to assert rows")
		return false, err
	}

	return len(rows) > 0, nil
}

type ConditionsParser struct {
	RawRows    interface{}
	RawColumns interface{}

	RawColumnsMap map[interface{}]interface{}
	Result        []interface{}
	Rows          interface{}
}

func NewConditionsParser() *ConditionsParser {
	return &ConditionsParser{}
}

func (p *ConditionsParser) Parse(rawRows, rawColumns interface{}, conditions string) (interface{}, error) {
	p.RawRows = rawRows
	p.RawColumns = rawColumns

	p.RawColumnsMap = make(map[interface{}]interface{})
	columns, ok := rawColumns.([]interface{})
	if ok {
		for _, v := range columns {
			column, ok := v.(map[string]interface{})
			if ok {
				p.RawColumnsMap[column["name"]] = column
			}
		}
	}

	lex := lexer.New(conditions, parseState)
	myLex := &MyLexer{lex}
	myLex.Start()
	parser := yyNewParser()
	parser.Parse(myLex, p)

	return p.Rows, nil
}

func parseState(l *lexer.L) lexer.StateFunc {
	if l.Peek() == lexer.EOFRune {
		return nil
	}

	// skip begin whitespace
	for {
		p := l.Peek()
		if p == lexer.EOFRune {
			return nil
		} else if p == ' ' || p == '\t' || p == '\n' || p == '\r' {
			l.Next()
			l.Ignore()
			continue
		}
		break
	}

	//find next whitespace or operator
	for {
		p := l.Peek()
		if p != ' ' && p != '=' && p != '<' && p != '>' && p != '!' && p != lexer.EOFRune {
			l.Next()
			continue
		}
		break
	}

	if len(l.Current()) == 0 {
		l.Take("!=<>")
	}

	str := strings.Trim(l.Current(), " ")
	if len(str) == 0 {
		return nil
	}

	if strings.ToLower(str) == "like" || strings.ToLower(str) == "and" || strings.ToLower(str) == "or" ||
		str == "=" || str == "!=" || str == "<>" || str == ">=" || str == ">" || str == "<=" || str == "<" {
		l.Emit(OpToken)
	} else if str[0] != '"' {
		l.Emit(StringToken)
	} else {
		if len(str) > 1 && str[len(str)-1] == '"' {
			l.Emit(QuotaStringToken)
		} else {
			for {
				p := l.Next()
				if p == lexer.EOFRune {
					l.Emit(InvalidToken)
					return nil
				} else if p != '"' {
					continue
				}
				break
			}
			l.Emit(QuotaStringToken)
		}
	}

	return parseState
}

func (l *MyLexer) Lex(lval *yySymType) int {
	tok, done := l.NextToken()

	if done {
		return 0
	}

	if tok.Type == StringToken {
		lval.Value = tok.Value
		return STRING
	} else if tok.Type == QuotaStringToken {
		lval.Value = tok.Value
		return QUOTE_STRING
	} else if tok.Type == OpToken {
		lval.Op = tok.Value
		if strings.ToLower(tok.Value) == "and" {
			return AND
		} else if strings.ToLower(tok.Value) == "or" {
			return OR
		} else if strings.ToLower(tok.Value) == "=" {
			return EQUAL
		} else if strings.ToLower(tok.Value) == "<>" {
			return NOT_EQUAL
		} else if strings.ToLower(tok.Value) == ">=" {
			return GTE
		} else if strings.ToLower(tok.Value) == ">" {
			return GT
		} else if strings.ToLower(tok.Value) == "<=" {
			return LTE
		} else if strings.ToLower(tok.Value) == "<" {
			return LT
		}
		return 0
	} else if tok.Type == InvalidToken {
		return 0
	}
	return 0
}

func (l *MyLexer) Error(s string) {
	fmt.Println(s)
}

func (p *ConditionsParser) Merge(leftRows interface{}, condition string, rightRows interface{}) interface{} {
	if rightRows == nil {
		return leftRows
	}

	var mergedRows []map[string]interface{}

	lRows, ok := leftRows.([]map[string]interface{})
	leftRowsIDList := make(map[interface{}]interface{})
	var val int
	if ok {
		for _, row := range lRows {
			leftRowsIDList[row["_id"]] = val
		}
	}
	if ok {
		if strings.ToLower(condition) == "and" {
			rRows, ok := rightRows.([]map[string]interface{})
			if ok {
				for _, row := range rRows {
					if _, ok := leftRowsIDList[row["_id"]]; ok {
						mergedRows = append(mergedRows, row)
					}
				}
			}
		} else if strings.ToLower(condition) == "or" {
			var ok bool
			mergedRows, ok = leftRows.([]map[string]interface{})
			if ok {
				rRows, ok := rightRows.([]map[string]interface{})
				if ok {
					for _, row := range rRows {
						if _, ok := leftRowsIDList[row["_id"]]; !ok {
							mergedRows = append(mergedRows, row)
						}
					}
				}
			}
		}
	}

	return mergedRows
}

func (p *ConditionsParser) Filter(columnName, condition, value string) interface{} {
	if !p.checkColumnExists(columnName) {
		return nil
	}
	column, ok := p.RawColumnsMap[columnName].(map[string]interface{})
	if !ok {
		return nil
	}
	columnType, ok := column["type"].(string)
	if !ok {
		return nil
	}
	columnObj := getColumnByType(ColumnTypes(columnType))
	inputValue := columnObj.ParseInputValue(value)

	var filterRows []map[string]interface{}

	if condition == "=" {
		rawRows, ok := p.RawRows.([]interface{})
		if ok {
			for _, v := range rawRows {
				row, ok := v.(map[string]interface{})
				if ok {
					cellValue, ok := row[columnName]
					if ok {
						if columnObj.ParseTableValue(cellValue).Equal(inputValue) {
							filterRows = append(filterRows, row)
						}
					}
				}
			}
		}
	} else if condition == "!=" || condition == "<>" {
		rawRows, ok := p.RawRows.([]interface{})
		if ok {
			for _, v := range rawRows {
				row, ok := v.(map[string]interface{})
				if ok {
					cellValue, ok := row[columnName]
					if ok {
						if columnObj.ParseTableValue(cellValue).UnEqual(inputValue) {
							filterRows = append(filterRows, row)
						}
					}
				}
			}
		}
	} else if condition == ">=" {
		rawRows, ok := p.RawRows.([]interface{})
		if ok {
			for _, v := range rawRows {
				row, ok := v.(map[string]interface{})
				if ok {
					cellValue, ok := row[columnName]
					if ok {
						if columnObj.ParseTableValue(cellValue).GreaterEqualThan(inputValue) {
							filterRows = append(filterRows, row)
						} else {
						}
					}
				}
			}
		}
	} else if condition == ">" {
		rawRows, ok := p.RawRows.([]interface{})
		if ok {
			for _, v := range rawRows {
				row, ok := v.(map[string]interface{})
				if ok {
					cellValue, ok := row[columnName]
					if ok {
						if columnObj.ParseTableValue(cellValue).GreaterThan(inputValue) {
							filterRows = append(filterRows, row)
						}
					}
				}
			}
		}
	} else if condition == "<=" {
		rawRows, ok := p.RawRows.([]interface{})
		if ok {
			for _, v := range rawRows {
				row, ok := v.(map[string]interface{})
				if ok {
					cellValue, ok := row[columnName]
					if ok {
						if columnObj.ParseTableValue(cellValue).LessEqualThan(inputValue) {
							filterRows = append(filterRows, row)
						}
					}
				}
			}
		}
	} else if condition == "<" {
		rawRows, ok := p.RawRows.([]interface{})
		if ok {
			for _, v := range rawRows {
				row, ok := v.(map[string]interface{})
				if ok {
					cellValue, ok := row[columnName]
					if ok {
						if columnObj.ParseTableValue(cellValue).LessThan(inputValue) {
							filterRows = append(filterRows, row)
						}
					}
				}
			}
		}
	} else if condition == "like" {
		rawRows, ok := p.RawRows.([]interface{})
		if ok {
			for _, v := range rawRows {
				row, ok := v.(map[string]interface{})
				if ok {
					cellValue, ok := row[columnName]
					if ok {
						if columnObj.ParseTableValue(cellValue).Like(inputValue) {
							filterRows = append(filterRows, row)
						}
					}
				}
			}
		}
	}

	return filterRows
}

func (p *ConditionsParser) checkColumnExists(column string) bool {
	if _, ok := p.RawColumnsMap[column]; ok {
		return true
	}

	return false
}
