package seatable_api

import (
	"fmt"
	"os"
	"testing"
)

const (
	serverURL = "https://cloud.seatable.io"
	token     = "448f4dd6e04fdb31f6f32ece7a03f9c641bba089"
	tableName = "table1"
)

var seatableAPI *SeaTableAPI
var rowID string

func TestMain(m *testing.M) {
	var err error
	seatableAPI, err = getSeaTableAPI()
	if err != nil {
		fmt.Printf("fail to get seatable API: %v", err)
		os.Exit(1)
	}
	code := m.Run()
	os.Exit(code)
}

func getSeaTableAPI() (*SeaTableAPI, error) {
	api := Init(token, serverURL)
	err := api.Auth(false)
	return api, err
}

func TestFilter(t *testing.T) {
	_, err := seatableAPI.Filter("table1", "age>=18 and sex=man", "")
	if err != nil {
		t.Errorf("failed to filter table: %v", err)
	}
}

func TestGet(t *testing.T) {
	_, err := seatableAPI.GetMetadata()
	if err != nil {
		t.Errorf("failed to get metadata: %v", err)
		t.FailNow()
	}
}

func TestPost(t *testing.T) {
	rowData := make(map[string]interface{})
	rowData["Name"] = "name1"
	rowData["age"] = 20
	ret, err := seatableAPI.AppendRow("table1", rowData)
	if err != nil {
		t.Errorf("failed to append row: %v", err)
	}
	rowID, _ = ret["_id"].(string)
}

func TestPut(t *testing.T) {
	rowData := make(map[string]interface{})
	rowData["Name"] = "name2"
	rowData["age"] = 10
	_, err := seatableAPI.UpdateRow(tableName, rowID, rowData)
	if err != nil {
		t.Errorf("failed to update row: %v", err)
	}
}

func TestDelete(t *testing.T) {
	_, err := seatableAPI.DeleteRow("table1", rowID)
	if err != nil {
		t.Errorf("failed to delete row: %v", err)
	}
}
