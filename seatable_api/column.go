package seatable_api

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

type Column interface {
	ParseInputValue(value string) interface{}
	ParseTableValue(value interface{}) ColumnValue
}

type ColumnValue interface {
	Equal(interface{}) bool
	UnEqual(interface{}) bool
	Like(interface{}) bool
	GreaterEqualThan(interface{}) bool
	GreaterThan(interface{}) bool
	LessEqualThan(interface{}) bool
	LessThan(interface{}) bool
}

func getColumnByType(columnType ColumnTypes) Column {
	if columnType == NUMBER {
		return &NumberColumn{NUMBER}
	} else if columnType == DATE {
		return &DateColumn{DATE}
	} else if columnType == CTIME {
		return &CTimeColumn{CTIME}
	} else if columnType == MTIME {
		return &MTimeColumn{MTIME}
	} else if columnType == CHECKBOX {
		return &CheckBoxColumn{CHECKBOX}
	} else if columnType == TEXT {
		return &TextColumn{TEXT}
	} else if columnType == MULTIPLE_SELECT {
		return &MultiSelectColumn{MULTIPLE_SELECT}
	} else if columnType == LONG_TEXT {
		return &LongTextColumn{LONG_TEXT}
	}

	return &TextColumn{TEXT}
}

type NumberColumn struct {
	ColumnType ColumnTypes
}

type NumberDateColumnValue struct {
	ColumnType ColumnTypes
	Value      interface{}
}

func (c *NumberColumn) ParseInputValue(value string) interface{} {
	if value == "" {
		return value
	}

	val, err := strconv.ParseFloat(value, 64)
	if err != nil {
		msg := fmt.Sprintf("failed to parse column value of float: %v", err)
		panic(msg)
	}

	return val
}

func (c *NumberColumn) ParseTableValue(v interface{}) ColumnValue {
	return &NumberDateColumnValue{c.ColumnType, v}
}

func (v *NumberDateColumnValue) Equal(value interface{}) bool {
	if _, ok := value.(string); ok {
		return v.Value == nil
	}

	return v.Value == value
}

func (v *NumberDateColumnValue) UnEqual(value interface{}) bool {
	if _, ok := value.(string); ok {
		return v.Value != nil
	}

	return v.Value != value
}

func (v *NumberDateColumnValue) Like(value interface{}) bool {
	msg := fmt.Sprintf("%s type column does not support the query method '%s'", v.ColumnType, "like")
	panic(msg)
}

func (v *NumberDateColumnValue) GreaterEqualThan(value interface{}) bool {
	lVal, ok := value.(float64)
	if !ok {
		panic("The token >, >=, <, <= does not support the null query string .")
	}

	if v.Value == nil {
		return false
	}

	rVal, ok := v.Value.(float64)
	if !ok {
		panic("The token >, >=, <, <= does not support the null query string .")
	}

	return rVal >= lVal
}

func (v *NumberDateColumnValue) GreaterThan(value interface{}) bool {
	lVal, ok := value.(float64)
	if !ok {
		panic("The token >, >=, <, <= does not support the null query string .")
	}

	if v.Value == nil {
		return false
	}

	rVal, ok := v.Value.(float64)
	if !ok {
		panic("The token >, >=, <, <= does not support the null query string .")
	}

	return rVal > lVal
}
func (v *NumberDateColumnValue) LessEqualThan(value interface{}) bool {
	lVal, ok := value.(float64)
	if !ok {
		panic("The token >, >=, <, <= does not support the null query string .")
	}

	if v.Value == nil {
		return false
	}

	rVal, ok := v.Value.(float64)
	if !ok {
		panic("The token >, >=, <, <= does not support the null query string .")
	}

	return rVal <= lVal
}

func (v *NumberDateColumnValue) LessThan(value interface{}) bool {
	lVal, ok := value.(float64)
	if !ok {
		panic("The token >, >=, <, <= does not support the null query string .")
	}

	if v.Value == nil {
		return false
	}

	rVal, ok := v.Value.(float64)
	if !ok {
		panic("The token >, >=, <, <= does not support the null query string .")
	}

	return rVal < lVal
}

type DateColumn struct {
	ColumnType ColumnTypes
}

func (c *DateColumn) ParseInputValue(value string) interface{} {
	if value == "" {
		return value
	}

	timeStrList := strings.Split(value, " ")
	if len(timeStrList) == 1 {
		t, err := time.Parse("2006-1-2", timeStrList[0])
		if err != nil {
			panic("failed to parse time")
		}
		return t
	} else if len(timeStrList) == 2 {
		hmsStrList := strings.Split(timeStrList[1], ":")
		if len(hmsStrList) == 1 {
			t, err := time.Parse("2006-1-2 15", value)
			if err != nil {
				panic("failed to parse time")
			}
			return t
		} else if len(timeStrList) == 2 {
			t, err := time.Parse("2006-1-2 15:04", value)
			if err != nil {
				panic("failed to parse time")
			}
			return t
		} else if len(timeStrList) == 3 {
			t, err := time.Parse("2006-1-2 15:04:05", value)
			if err != nil {
				panic("failed to parse time")
			}
			return t
		}
	}

	return nil
}

func (c *DateColumn) ParseTableValue(v interface{}) ColumnValue {
	val, ok := v.(string)
	if !ok {
		panic("failed to assert")
	}
	return &NumberDateColumnValue{c.ColumnType, c.ParseInputValue(val)}
}

type CTimeColumn struct {
	ColumnType ColumnTypes
}

func (c *CTimeColumn) ParseInputValue(value string) interface{} {
	if value == "" {
		return value
	}

	timeStrList := strings.Split(value, " ")
	if len(timeStrList) == 1 {
		t, err := time.Parse("2006-1-2", timeStrList[0])
		if err != nil {
			panic("failed to parse time")
		}
		return t
	} else if len(timeStrList) == 2 {
		hmsStrList := strings.Split(timeStrList[1], ":")
		if len(hmsStrList) == 1 {
			t, err := time.Parse("2006-1-2 15", value)
			if err != nil {
				panic("failed to parse time")
			}
			return t
		} else if len(timeStrList) == 2 {
			t, err := time.Parse("2006-1-2 15:04", value)
			if err != nil {
				panic("failed to parse time")
			}
			return t
		} else if len(timeStrList) == 3 {
			t, err := time.Parse("2006-1-2 15:04:05", value)
			if err != nil {
				panic("failed to parse time")
			}
			return t
		}
	}

	return nil
}

func (c *CTimeColumn) ParseTableValue(v interface{}) ColumnValue {
	val, ok := v.(string)
	if !ok {
		panic("failed to assert")
	}
	return &NumberDateColumnValue{c.ColumnType, c.GetLocalTime(val)}
}

func (c *CTimeColumn) GetLocalTime(v string) interface{} {
	t, err := time.Parse("2006-1-2T15.999999+00:00", v)
	if err != nil {
		panic("failed to parse time")
	}

	_, off := time.Now().Zone()
	localTime := t.Add(time.Duration(off) * time.Second)

	return localTime
}

type MTimeColumn struct {
	ColumnType ColumnTypes
}

func (c *MTimeColumn) ParseInputValue(value string) interface{} {
	if value == "" {
		return value
	}

	timeStrList := strings.Split(value, " ")
	if len(timeStrList) == 1 {
		t, err := time.Parse("2006-1-2", timeStrList[0])
		if err != nil {
			panic("failed to parse time")
		}
		return t
	} else if len(timeStrList) == 2 {
		hmsStrList := strings.Split(timeStrList[1], ":")
		if len(hmsStrList) == 1 {
			t, err := time.Parse("2006-1-2 15", value)
			if err != nil {
				panic("failed to parse time")
			}
			return t
		} else if len(timeStrList) == 2 {
			t, err := time.Parse("2006-1-2 15:04", value)
			if err != nil {
				panic("failed to parse time")
			}
			return t
		} else if len(timeStrList) == 3 {
			t, err := time.Parse("2006-1-2 15:04:05", value)
			if err != nil {
				panic("failed to parse time")
			}
			return t
		}
	}

	return nil
}

func (c *MTimeColumn) ParseTableValue(v interface{}) ColumnValue {
	val, ok := v.(string)
	if !ok {
		panic("failed to assert")
	}
	return &NumberDateColumnValue{c.ColumnType, c.GetLocalTime(val)}
}

func (c *MTimeColumn) GetLocalTime(v string) interface{} {
	t, err := time.Parse("2006-1-2T15.999999+00:00", v)
	if err != nil {
		panic("failed to parse time")
	}

	_, off := time.Now().Zone()
	localTime := t.Add(time.Duration(off) * time.Second)

	return localTime
}

type CheckBoxColumn struct {
	ColumnType ColumnTypes
}

type BoolColumnValue struct {
	ColumnType ColumnTypes
	Value      interface{}
}

func (c *CheckBoxColumn) ParseInputValue(value string) interface{} {
	if value == "" {
		return false
	}
	if strings.ToLower(value) == "true" {
		return true
	} else if strings.ToLower(value) == "false" {
		return false
	}
	msg := fmt.Sprintf("%s type column does not support the query string as \"%s\","+
		"the supported query string pattern like:"+
		"\"true\" or \"false\", case insensitive", CHECKBOX, value)
	panic(msg)

	return nil
}

func (c *CheckBoxColumn) ParseTableValue(v interface{}) ColumnValue {
	return &BoolColumnValue{c.ColumnType, v}
}

func (c *CheckBoxColumn) GetLocalTime(v string) interface{} {
	t, err := time.Parse("2006-1-2T15.999999+00:00", v)
	if err != nil {
		panic("failed to parse time")
	}

	_, off := time.Now().Zone()
	localTime := t.Add(time.Duration(off) * time.Second)

	return localTime
}

func (v *BoolColumnValue) Equal(value interface{}) bool {
	if _, ok := value.(bool); !ok {
		panic("input value isn't bool")
	}

	return v.Value == value
}

func (v *BoolColumnValue) UnEqual(value interface{}) bool {
	if _, ok := value.(bool); !ok {
		panic("input value isn't bool")
	}

	return v.Value != value
}

func (v *BoolColumnValue) Like(value interface{}) bool {
	msg := fmt.Sprintf("%s type column does not support the query method '%s'", v.ColumnType, "like")
	panic(msg)
}

func (v *BoolColumnValue) GreaterEqualThan(value interface{}) bool {
	msg := fmt.Sprintf("%s type column does not support the query method '%s'", v.ColumnType, ">=")
	panic(msg)
}

func (v *BoolColumnValue) GreaterThan(value interface{}) bool {
	msg := fmt.Sprintf("%s type column does not support the query method '%s'", v.ColumnType, ">")
	panic(msg)
}
func (v *BoolColumnValue) LessEqualThan(value interface{}) bool {
	msg := fmt.Sprintf("%s type column does not support the query method '%s'", v.ColumnType, "<=")
	panic(msg)
}

func (v *BoolColumnValue) LessThan(value interface{}) bool {
	msg := fmt.Sprintf("%s type column does not support the query method '%s'", v.ColumnType, "<")
	panic(msg)
}

type TextColumn struct {
	ColumnType ColumnTypes
}

type StringColumnValue struct {
	ColumnType ColumnTypes
	Value      interface{}
}

func (c *TextColumn) ParseInputValue(value string) interface{} {
	return value
}

func (c *TextColumn) ParseTableValue(v interface{}) ColumnValue {
	return &StringColumnValue{c.ColumnType, v}
}

func (v *StringColumnValue) Equal(value interface{}) bool {
	val, ok := value.(string)
	if !ok {
		panic("input value isn't string")
	}

	if val == "" {
		return v.Value == nil
	}

	return v.Value == value
}

func (v *StringColumnValue) UnEqual(value interface{}) bool {
	val, ok := value.(string)
	if !ok {
		panic("input value isn't string")
	}

	if val == "" {
		return v.Value != nil
	}

	return v.Value != value
}

func (v *StringColumnValue) Like(value interface{}) bool {
	val, ok := value.(string)
	if !ok {
		panic("input value isn't string")
	}
	if strings.Index(val, "%") < 0 {
		msg := fmt.Sprintf("There is no patterns found in \"like\" phrases")
		panic(msg)
	}
	if string(val[0]) != "%" && string(val[len(val)-1]) == "%" {
		start := val[:len(val)-1]
		data, ok := v.Value.(string)
		if ok {
			return strings.HasPrefix(data, start)
		}
		return false
	} else if string(val[0]) == "%" && string(val[len(val)-1]) != "%" {
		end := val[1:]
		data, ok := v.Value.(string)
		if ok {
			return strings.HasSuffix(data, end)
		}
		return false
	} else if string(val[0]) == "%" && string(val[len(val)-1]) == "%" {
		middle := val[1 : len(val)-1]
		data, ok := v.Value.(string)
		if ok {
			return strings.Index(data, middle) >= 0
		}
		return false
	}
	valueList := strings.Split(val, "%")
	start := valueList[0]
	end := valueList[len(valueList)-1]
	data, ok := v.Value.(string)
	if ok {
		return strings.HasPrefix(data, start) && strings.HasSuffix(data, end)
	}

	return false
}

func (v *StringColumnValue) GreaterEqualThan(value interface{}) bool {
	msg := fmt.Sprintf("%s type column does not support the query method '%s'", v.ColumnType, ">=")
	panic(msg)
}

func (v *StringColumnValue) GreaterThan(value interface{}) bool {
	msg := fmt.Sprintf("%s type column does not support the query method '%s'", v.ColumnType, ">")
	panic(msg)
}

func (v *StringColumnValue) LessEqualThan(value interface{}) bool {
	msg := fmt.Sprintf("%s type column does not support the query method '%s'", v.ColumnType, "<=")
	panic(msg)
}

func (v *StringColumnValue) LessThan(value interface{}) bool {
	msg := fmt.Sprintf("%s type column does not support the query method '%s'", v.ColumnType, "<")
	panic(msg)
}

type MultiSelectColumn struct {
	ColumnType ColumnTypes
}

type ListColumnValue struct {
	ColumnType ColumnTypes
	Value      interface{}
}

func (c *MultiSelectColumn) ParseInputValue(value string) interface{} {
	return value
}

func (c *MultiSelectColumn) ParseTableValue(v interface{}) ColumnValue {
	return &ListColumnValue{c.ColumnType, v}
}

func (v *ListColumnValue) Equal(value interface{}) bool {
	if value == nil {
		return v.Value == nil
	}
	list, ok := v.Value.([]interface{})
	if ok {
		for _, val := range list {
			if value == val {
				return true
			}
		}

		return false
	}

	panic("failed to assert list")
	return false
}

func (v *ListColumnValue) UnEqual(value interface{}) bool {
	if value == nil {
		return v.Value != nil
	}
	list, ok := v.Value.([]interface{})
	if ok {
		for _, val := range list {
			if value == val {
				return false
			}
		}

		return true
	}

	panic("failed to assert list")
	return true
}

func (v *ListColumnValue) Like(value interface{}) bool {
	msg := fmt.Sprintf("%s type column does not support the query method '%s'", v.ColumnType, "like")
	panic(msg)
}

func (v *ListColumnValue) GreaterEqualThan(value interface{}) bool {
	msg := fmt.Sprintf("%s type column does not support the query method '%s'", v.ColumnType, ">=")
	panic(msg)
}

func (v *ListColumnValue) GreaterThan(value interface{}) bool {
	msg := fmt.Sprintf("%s type column does not support the query method '%s'", v.ColumnType, ">")
	panic(msg)
}
func (v *ListColumnValue) LessEqualThan(value interface{}) bool {
	msg := fmt.Sprintf("%s type column does not support the query method '%s'", v.ColumnType, "<=")
	panic(msg)
}

func (v *ListColumnValue) LessThan(value interface{}) bool {
	msg := fmt.Sprintf("%s type column does not support the query method '%s'", v.ColumnType, "<")
	panic(msg)
}

type LongTextColumn struct {
	ColumnType ColumnTypes
}

func (c *LongTextColumn) ParseInputValue(value string) interface{} {
	return value
}

func (c *LongTextColumn) ParseTableValue(v interface{}) ColumnValue {
	value, ok := v.(string)
	if !ok {
		panic("input value isn't a string")
	}
	val := strings.Trim(value, "\n")
	return &StringColumnValue{c.ColumnType, val}
}
