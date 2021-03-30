package seatable_api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	neturl "net/url"
	"os"
	"strings"
	"time"
)

type Base struct {
	Token           string
	ServerURL       string
	DtableServerURL string
	JwtToken        string
	JwtExp          int64
	Headers         map[string]string
	WorkspaceID     string
	DtableUUID      string
	DtableName      string
	Timeout         int
	Client          *SocketIO
}

func Init(token string, serverURL string) *Base {
	return &Base{Token: token, ServerURL: serverURL, Timeout: 30}
}

func (s *Base) Auth(withSocketIO bool) error {
	s.JwtExp = time.Now().Add(72 * time.Hour).Unix()
	url := s.ServerURL + "/api/v2.1/dtable/app-access-token/"
	Headers := makeHeaders(s.Token)
	status, body, err := httpGet(url, "", Headers, s.Timeout, nil)
	if err != nil {
		err := fmt.Errorf("failed to request url: %s: %v", url, err)
		return err
	}

	if status >= 400 {
		err := fmt.Errorf("bad response for GET: %d", status)
		return err
	}

	rsp, err := parseResponse(body)
	if err != nil {
		err := fmt.Errorf("failed to parse response: %v", err)
		return err
	}

	ret, ok := rsp.(map[string]interface{})
	if !ok {
		err := fmt.Errorf("failed to assert response")
		return err
	}

	serverURL, ok := ret["dtable_server"].(string)
	if ok {
		s.DtableServerURL = parseServerURL(serverURL)
	}

	accessToken, ok := ret["access_token"].(string)
	if ok {
		s.JwtToken = accessToken
		s.Headers = makeHeaders(accessToken)
	}

	workspaceID, ok := ret["workspace_id"].(string)
	if ok {
		s.WorkspaceID = workspaceID
	}

	dtableUUID, ok := ret["dtable_uuid"].(string)
	if ok {
		s.DtableUUID = dtableUUID
	}

	dtableName, ok := ret["dtable_name"].(string)
	if ok {
		s.DtableName = dtableName
	}

	if withSocketIO {
		base, err := s.Clone()
		if err != nil {
			err := fmt.Errorf("failed to clone base: %v", err)
			return err
		}
		client, err := InitSocketIO(base)
		if err != nil {
			err := fmt.Errorf("failed to init socket io: %v", err)
			return err
		}
		s.Client = client
	}
	return nil
}

func (s *Base) Clone() (*Base, error) {
	var dst = new(Base)
	b, err := json.Marshal(s)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(b, dst)
	return dst, err
}

func parseServerURL(serverURL string) string {
	return strings.TrimRight(serverURL, "/")
}

func (s *Base) GetMetadata() (interface{}, error) {
	url := s.DtableServerURL + "/api/v1/dtables/" + s.DtableUUID + "/metadata/"

	status, body, err := httpGet(url, "", s.Headers, s.Timeout, nil)
	if err != nil {
		err := fmt.Errorf("failed to request url: %s: %v", url, err)
		return nil, err
	}

	if status >= 400 {
		err := fmt.Errorf("bad response for GET: %d", status)
		return nil, err
	}

	rsp, err := parseResponse(body)
	if err != nil {
		err := fmt.Errorf("failed to parse response: %v", err)
		return nil, err
	}

	ret, ok := rsp.(map[string]interface{})
	if !ok {
		err := fmt.Errorf("failed to assert response")
		return nil, err
	}

	return ret["metadata"], nil
}

func (s *Base) AppendRow(tableName string, rowData interface{}) (map[string]interface{}, error) {
	url := s.DtableServerURL + "/api/v1/dtables/" + s.DtableUUID + "/rows/"

	data := make(map[string]interface{})
	data["table_name"] = tableName
	data["row"] = rowData

	jsonStr, err := json.Marshal(data)
	if err != nil {
		err := fmt.Errorf("failed to encode post data: %v", err)
		return nil, err
	}

	status, body, err := httpPost(url, s.Headers, bytes.NewBuffer(jsonStr), s.Timeout)
	if err != nil {
		err := fmt.Errorf("failed to post row to %s: %v", url, err)
		return nil, err
	}

	if status >= 400 {
		err := fmt.Errorf("bad response for POST: %d", status)
		return nil, err
	}

	rsp, err := parseResponse(body)
	if err != nil {
		err := fmt.Errorf("failed to parse response: %v", err)
		return nil, err
	}

	ret, ok := rsp.(map[string]interface{})
	if !ok {
		err := fmt.Errorf("failed to assert response")
		return nil, err
	}

	return ret, nil
}

func (s *Base) BatchAppendRows(tableName string, rowsData []interface{}) (map[string]interface{}, error) {
	url := s.DtableServerURL + "/api/v1/dtables/" + s.DtableUUID + "/batch-append-rows/"

	data := make(map[string]interface{})
	data["table_name"] = tableName
	data["rows"] = rowsData

	jsonStr, err := json.Marshal(data)
	if err != nil {
		err := fmt.Errorf("failed to encode post data: %v", err)
		return nil, err
	}

	status, body, err := httpPost(url, s.Headers, bytes.NewBuffer(jsonStr), s.Timeout)
	if err != nil {
		err := fmt.Errorf("failed to post rows to %s: %v", url, err)
		return nil, err
	}

	if status >= 400 {
		err := fmt.Errorf("bad response for POST: %d", status)
		return nil, err
	}

	rsp, err := parseResponse(body)
	if err != nil {
		err := fmt.Errorf("failed to parse response: %v", err)
		return nil, err
	}

	ret, ok := rsp.(map[string]interface{})
	if !ok {
		err := fmt.Errorf("failed to assert response")
		return nil, err
	}

	return ret, nil
}

func (s *Base) InsertRow(tableName string, rowData interface{}, anchorRowID string) (map[string]interface{}, error) {
	url := s.DtableServerURL + "/api/v1/dtables/" + s.DtableUUID + "/rows/"

	data := make(map[string]interface{})
	data["table_name"] = tableName
	data["row"] = rowData
	data["anchor_row_id"] = anchorRowID

	jsonStr, err := json.Marshal(data)
	if err != nil {
		err := fmt.Errorf("failed to encode post data: %v", err)
		return nil, err
	}

	status, body, err := httpPost(url, s.Headers, bytes.NewBuffer(jsonStr), s.Timeout)
	if err != nil {
		err := fmt.Errorf("failed to post row to %s: %v", url, err)
		return nil, err
	}

	if status >= 400 {
		err := fmt.Errorf("bad response for POST: %d", status)
		return nil, err
	}

	rsp, err := parseResponse(body)
	if err != nil {
		err := fmt.Errorf("failed to parse response: %v", err)
		return nil, err
	}

	ret, ok := rsp.(map[string]interface{})
	if !ok {
		err := fmt.Errorf("failed to assert response")
		return nil, err
	}

	return ret, nil
}

func (s *Base) UpdateRow(tableName string, rowID string, rowData interface{}) (map[string]interface{}, error) {
	url := s.DtableServerURL + "/api/v1/dtables/" + s.DtableUUID + "/rows/"

	data := make(map[string]interface{})
	data["table_name"] = tableName
	data["row_id"] = rowID
	data["row"] = rowData

	jsonStr, err := json.Marshal(data)
	if err != nil {
		err := fmt.Errorf("failed to encode put data: %v", err)
		return nil, err
	}

	status, body, err := httpPut(url, s.Headers, bytes.NewBuffer(jsonStr), s.Timeout)
	if err != nil {
		err := fmt.Errorf("failed to post row to %s: %v", url, err)
		return nil, err
	}

	if status >= 400 {
		err := fmt.Errorf("bad response for POST: %d", status)
		return nil, err
	}

	rsp, err := parseResponse(body)
	if err != nil {
		err := fmt.Errorf("failed to parse response: %v", err)
		return nil, err
	}

	ret, ok := rsp.(map[string]interface{})
	if !ok {
		err := fmt.Errorf("failed to assert response")
		return nil, err
	}

	return ret, nil
}

func (s *Base) DeleteRow(tableName, rowID string) (map[string]interface{}, error) {
	url := s.DtableServerURL + "/api/v1/dtables/" + s.DtableUUID + "/rows/"

	data := make(map[string]interface{})
	data["table_name"] = tableName
	data["row_id"] = rowID

	jsonStr, err := json.Marshal(data)
	if err != nil {
		err := fmt.Errorf("failed to encode json data: %v", err)
		return nil, err
	}

	status, body, err := httpDelete(url, s.Headers, bytes.NewBuffer(jsonStr), s.Timeout)
	if err != nil {
		err := fmt.Errorf("failed to post row to %s: %v", url, err)
		return nil, err
	}

	if status >= 400 {
		err := fmt.Errorf("bad response for POST: %d", status)
		return nil, err
	}

	rsp, err := parseResponse(body)
	if err != nil {
		err := fmt.Errorf("failed to parse response: %v", err)
		return nil, err
	}

	ret, ok := rsp.(map[string]interface{})
	if !ok {
		err := fmt.Errorf("failed to assert response")
		return nil, err
	}

	return ret, nil
}

func (s *Base) BatchDeleteRows(tableName string, rowIDs interface{}) (map[string]interface{}, error) {
	url := s.DtableServerURL + "/api/v1/dtables/" + s.DtableUUID + "/batch-delete-rows/"

	data := make(map[string]interface{})
	data["table_name"] = tableName
	data["row_ids"] = rowIDs

	jsonStr, err := json.Marshal(data)
	if err != nil {
		err := fmt.Errorf("failed to encode json data: %v", err)
		return nil, err
	}

	status, body, err := httpDelete(url, s.Headers, bytes.NewBuffer(jsonStr), s.Timeout)
	if err != nil {
		err := fmt.Errorf("failed to post row to %s: %v", url, err)
		return nil, err
	}

	if status >= 400 {
		err := fmt.Errorf("bad response for POST: %d", status)
		return nil, err
	}

	rsp, err := parseResponse(body)
	if err != nil {
		err := fmt.Errorf("failed to parse response: %v", err)
		return nil, err
	}

	ret, ok := rsp.(map[string]interface{})
	if !ok {
		err := fmt.Errorf("failed to assert response")
		return nil, err
	}

	return ret, nil
}

func (s *Base) FilterRows(tableName string, filters []map[string]interface{}, viewName string, filterConjunction string) (interface{}, error) {
	if filters == nil {
		err := fmt.Errorf("filters can not be empty")
		return nil, err
	}

	for _, v := range filters {
		for k, _ := range v {
			hasKey := false
			for _, key := range ROW_FILTER_KEYS {
				if k == key {
					hasKey = true
					break
				}
			}
			if !hasKey {
				err := fmt.Errorf("filters invalid")
				return nil, err
			}
		}
	}

	if filterConjunction != "And" && filterConjunction != "Or" {
		err := fmt.Errorf("filter_conjunction invalid, filter_conjunction must be \"And\" or \"Or\"")
		return nil, err
	}

	params := neturl.Values{}
	params.Add("table_name", tableName)

	data := make(map[string]interface{})
	data["filters"] = filters
	data["filter_conjunction"] = filterConjunction

	jsonStr, err := json.Marshal(data)
	if err != nil {
		err := fmt.Errorf("failed to encode json data: %v", err)
		return nil, err
	}

	url := s.DtableServerURL + "/api/v1/dtables/" + s.DtableUUID + "/filtered-rows/"

	status, body, err := httpGet(url, params.Encode(), s.Headers, s.Timeout, bytes.NewBuffer(jsonStr))
	if err != nil {
		err := fmt.Errorf("failed to request url: %s: %v", url, err)
		return nil, err
	}

	if status >= 400 {
		err := fmt.Errorf("bad response for GET: %d", status)
		return nil, err
	}

	rsp, err := parseResponse(body)
	if err != nil {
		err := fmt.Errorf("failed to parse response: %v", err)
		return nil, err
	}

	ret, ok := rsp.(map[string]interface{})
	if !ok {
		err := fmt.Errorf("failed to assert response")
		return nil, err
	}

	return ret["rows"], nil
}

func (s *Base) GetFileDownloadLink(path string) (interface{}, error) {
	url := s.ServerURL + "/api/v2.1/dtable/app-download-link/"

	params := neturl.Values{}
	params.Add("path", path)

	status, body, err := httpGet(url, params.Encode(), s.Headers, s.Timeout, nil)
	if err != nil {
		err := fmt.Errorf("failed to request url: %s: %v", url, err)
		return nil, err
	}

	if status >= 400 {
		err := fmt.Errorf("bad response for GET: %d", status)
		return nil, err
	}

	rsp, err := parseResponse(body)
	if err != nil {
		err := fmt.Errorf("failed to parse response: %v", err)
		return nil, err
	}

	ret, ok := rsp.(map[string]interface{})
	if !ok {
		err := fmt.Errorf("failed to assert response")
		return nil, err
	}

	return ret["download_link"], nil
}

func (s *Base) GetFileUploadLink() (map[string]interface{}, error) {
	url := s.ServerURL + "/api/v2.1/dtable/app-upload-link/"

	status, body, err := httpGet(url, "", s.Headers, s.Timeout, nil)
	if err != nil {
		err := fmt.Errorf("failed to request url: %s: %v", url, err)
		return nil, err
	}

	if status >= 400 {
		err := fmt.Errorf("bad response for GET: %d", status)
		return nil, err
	}

	rsp, err := parseResponse(body)
	if err != nil {
		err := fmt.Errorf("failed to parse response: %v", err)
		return nil, err
	}

	ret, ok := rsp.(map[string]interface{})
	if !ok {
		err := fmt.Errorf("failed to assert response")
		return nil, err
	}

	return ret, nil
}

func (s *Base) AddLink(linkID, tableName, otherTableName, rowID, otherRowID string) (map[string]interface{}, error) {
	url := s.DtableServerURL + "/api/v1/dtables/" + s.DtableUUID + "/links/"

	data := make(map[string]interface{})
	data["link_id"] = linkID
	data["table_name"] = tableName
	data["other_table_name"] = otherTableName
	data["table_row_id"] = rowID
	data["other_table_row_id"] = otherRowID

	jsonStr, err := json.Marshal(data)
	if err != nil {
		err := fmt.Errorf("failed to encode post data: %v", err)
		return nil, err
	}

	status, body, err := httpPost(url, s.Headers, bytes.NewBuffer(jsonStr), s.Timeout)
	if err != nil {
		err := fmt.Errorf("failed to post row to %s: %v", url, err)
		return nil, err
	}

	if status >= 400 {
		err := fmt.Errorf("bad response for POST: %d", status)
		return nil, err
	}

	rsp, err := parseResponse(body)
	if err != nil {
		err := fmt.Errorf("failed to parse response: %v", err)
		return nil, err
	}

	ret, ok := rsp.(map[string]interface{})
	if !ok {
		err := fmt.Errorf("failed to assert response")
		return nil, err
	}

	return ret, nil
}

func (s *Base) RemoveLink(linkID, tableName, otherTableName, rowID, otherRowID string) (map[string]interface{}, error) {
	url := s.DtableServerURL + "/api/v1/dtables/" + s.DtableUUID + "/links/"

	data := make(map[string]interface{})
	data["link_id"] = linkID
	data["table_name"] = tableName
	data["other_table_name"] = otherTableName
	data["table_row_id"] = rowID
	data["other_table_row_id"] = otherRowID

	jsonStr, err := json.Marshal(data)
	if err != nil {
		err := fmt.Errorf("failed to encode json data: %v", err)
		return nil, err
	}

	status, body, err := httpDelete(url, s.Headers, bytes.NewBuffer(jsonStr), s.Timeout)
	if err != nil {
		err := fmt.Errorf("failed to post row to %s: %v", url, err)
		return nil, err
	}

	if status >= 400 {
		err := fmt.Errorf("bad response for POST: %d", status)
		return nil, err
	}

	rsp, err := parseResponse(body)
	if err != nil {
		err := fmt.Errorf("failed to parse response: %v", err)
		return nil, err
	}

	ret, ok := rsp.(map[string]interface{})
	if !ok {
		err := fmt.Errorf("failed to assert response")
		return nil, err
	}

	return ret, nil
}

func (s *Base) GetColumnLinkID(tableName, columnName, viewName string) (interface{}, error) {
	columns, err := s.ListColumns(tableName, viewName)
	if err != nil {
		return nil, err
	}

	lists, ok := columns.([]map[string]interface{})
	if ok {
		for _, column := range lists {
			if column["name"] == columnName && column["type"] == "link" {
				data, ok := column["data"].(map[string]interface{})
				if ok {
					return data["link_id"], nil
				}
			}
		}
	}

	return nil, nil
}

func (s *Base) ListColumns(tableName, viewName string) (interface{}, error) {
	url := s.DtableServerURL + "/api/v1/dtables/" + s.DtableUUID + "/columns/"

	params := neturl.Values{}
	params.Add("table_name", tableName)
	if viewName != "" {
		params.Add("view_name", viewName)
	}

	status, body, err := httpGet(url, params.Encode(), s.Headers, s.Timeout, nil)
	if err != nil {
		err := fmt.Errorf("failed to request url: %s: %v", url, err)
		return nil, err
	}

	if status >= 400 {
		err := fmt.Errorf("bad response for GET: %d", status)
		return nil, err
	}

	rsp, err := parseResponse(body)
	if err != nil {
		err := fmt.Errorf("failed to parse response: %v", err)
		return nil, err
	}

	ret, ok := rsp.(map[string]interface{})
	if !ok {
		err := fmt.Errorf("failed to assert response")
		return nil, err
	}

	return ret["columns"], nil
}

func (s *Base) InsertColumn(tableName, columnName string, columnType ColumnTypes, columnKey string) (map[string]interface{}, error) {
	url := s.DtableServerURL + "/api/v1/dtables/" + s.DtableUUID + "/columns/"

	data := make(map[string]interface{})
	data["table_name"] = tableName
	data["column_name"] = columnName
	data["column_type"] = columnType
	if columnKey != "" {
		data["column_key"] = columnKey
	}

	jsonStr, err := json.Marshal(data)
	if err != nil {
		err := fmt.Errorf("failed to encode post data: %v", err)
		return nil, err
	}

	status, body, err := httpPost(url, s.Headers, bytes.NewBuffer(jsonStr), s.Timeout)
	if err != nil {
		err := fmt.Errorf("failed to post row to %s: %v", url, err)
		return nil, err
	}

	if status >= 400 {
		err := fmt.Errorf("bad response for POST: %d", status)
		return nil, err
	}

	rsp, err := parseResponse(body)
	if err != nil {
		err := fmt.Errorf("failed to parse response: %v", err)
		return nil, err
	}

	ret, ok := rsp.(map[string]interface{})
	if !ok {
		err := fmt.Errorf("failed to assert response")
		return nil, err
	}

	return ret, nil
}

func (s *Base) RenameColumn(tableName, columnKey, newColumnName string) (map[string]interface{}, error) {
	url := s.DtableServerURL + "/api/v1/dtables/" + s.DtableUUID + "/columns/"

	data := make(map[string]interface{})
	data["op_type"] = RENAME_COLUMN
	data["table_name"] = tableName
	data["column_key"] = columnKey
	data["new_column_name"] = newColumnName

	jsonStr, err := json.Marshal(data)
	if err != nil {
		err := fmt.Errorf("failed to encode put data: %v", err)
		return nil, err
	}

	status, body, err := httpPut(url, s.Headers, bytes.NewBuffer(jsonStr), s.Timeout)
	if err != nil {
		err := fmt.Errorf("failed to post row to %s: %v", url, err)
		return nil, err
	}

	if status >= 400 {
		err := fmt.Errorf("bad response for POST: %d", status)
		return nil, err
	}

	rsp, err := parseResponse(body)
	if err != nil {
		err := fmt.Errorf("failed to parse response: %v", err)
		return nil, err
	}

	ret, ok := rsp.(map[string]interface{})
	if !ok {
		err := fmt.Errorf("failed to assert response")
		return nil, err
	}

	return ret, nil
}

func (s *Base) ResizeColumn(tableName, columnKey string, newColumnWidth int) (map[string]interface{}, error) {
	url := s.DtableServerURL + "/api/v1/dtables/" + s.DtableUUID + "/columns/"

	data := make(map[string]interface{})
	data["op_type"] = RESIZE_COLUMN
	data["table_name"] = tableName
	data["column_key"] = columnKey
	data["new_column_width"] = newColumnWidth

	jsonStr, err := json.Marshal(data)
	if err != nil {
		err := fmt.Errorf("failed to encode put data: %v", err)
		return nil, err
	}

	status, body, err := httpPut(url, s.Headers, bytes.NewBuffer(jsonStr), s.Timeout)
	if err != nil {
		err := fmt.Errorf("failed to post row to %s: %v", url, err)
		return nil, err
	}

	if status >= 400 {
		err := fmt.Errorf("bad response for POST: %d", status)
		return nil, err
	}

	rsp, err := parseResponse(body)
	if err != nil {
		err := fmt.Errorf("failed to parse response: %v", err)
		return nil, err
	}

	ret, ok := rsp.(map[string]interface{})
	if !ok {
		err := fmt.Errorf("failed to assert response")
		return nil, err
	}

	return ret, nil
}

func (s *Base) FreezeColumn(tableName, columnKey string, frozen bool) (map[string]interface{}, error) {
	url := s.DtableServerURL + "/api/v1/dtables/" + s.DtableUUID + "/columns/"

	data := make(map[string]interface{})
	data["op_type"] = FREEZE_COLUMN
	data["table_name"] = tableName
	data["column_key"] = columnKey
	data["frozen"] = frozen

	jsonStr, err := json.Marshal(data)
	if err != nil {
		err := fmt.Errorf("failed to encode put data: %v", err)
		return nil, err
	}

	status, body, err := httpPut(url, s.Headers, bytes.NewBuffer(jsonStr), s.Timeout)
	if err != nil {
		err := fmt.Errorf("failed to post row to %s: %v", url, err)
		return nil, err
	}

	if status >= 400 {
		err := fmt.Errorf("bad response for POST: %d", status)
		return nil, err
	}

	rsp, err := parseResponse(body)
	if err != nil {
		err := fmt.Errorf("failed to parse response: %v", err)
		return nil, err
	}

	ret, ok := rsp.(map[string]interface{})
	if !ok {
		err := fmt.Errorf("failed to assert response")
		return nil, err
	}

	return ret, nil
}

func (s *Base) moveColumn(tableName, columnKey string, targetColumnKey bool) (map[string]interface{}, error) {
	url := s.DtableServerURL + "/api/v1/dtables/" + s.DtableUUID + "/columns/"

	data := make(map[string]interface{})
	data["op_type"] = MOVE_COLUMN
	data["table_name"] = tableName
	data["column_key"] = columnKey
	data["target_column_key"] = targetColumnKey

	jsonStr, err := json.Marshal(data)
	if err != nil {
		err := fmt.Errorf("failed to encode put data: %v", err)
		return nil, err
	}

	status, body, err := httpPut(url, s.Headers, bytes.NewBuffer(jsonStr), s.Timeout)
	if err != nil {
		err := fmt.Errorf("failed to post row to %s: %v", url, err)
		return nil, err
	}

	if status >= 400 {
		err := fmt.Errorf("bad response for POST: %d", status)
		return nil, err
	}

	rsp, err := parseResponse(body)
	if err != nil {
		err := fmt.Errorf("failed to parse response: %v", err)
		return nil, err
	}

	ret, ok := rsp.(map[string]interface{})
	if !ok {
		err := fmt.Errorf("failed to assert response")
		return nil, err
	}

	return ret, nil
}

func (s *Base) ModifyColumnType(tableName, columnKey string, newColumnType ColumnTypes) (map[string]interface{}, error) {
	url := s.DtableServerURL + "/api/v1/dtables/" + s.DtableUUID + "/columns/"

	data := make(map[string]interface{})
	data["op_type"] = MODIFY_COLUMN_TYPE
	data["table_name"] = tableName
	data["column_key"] = columnKey
	data["new_column_type"] = newColumnType

	jsonStr, err := json.Marshal(data)
	if err != nil {
		err := fmt.Errorf("failed to encode put data: %v", err)
		return nil, err
	}

	status, body, err := httpPut(url, s.Headers, bytes.NewBuffer(jsonStr), s.Timeout)
	if err != nil {
		err := fmt.Errorf("failed to post row to %s: %v", url, err)
		return nil, err
	}

	if status >= 400 {
		err := fmt.Errorf("bad response for POST: %d", status)
		return nil, err
	}

	rsp, err := parseResponse(body)
	if err != nil {
		err := fmt.Errorf("failed to parse response: %v", err)
		return nil, err
	}

	ret, ok := rsp.(map[string]interface{})
	if !ok {
		err := fmt.Errorf("failed to assert response")
		return nil, err
	}

	return ret, nil
}

func (s *Base) DeleteColumn(tableName, columnKey string) (map[string]interface{}, error) {
	url := s.DtableServerURL + "/api/v1/dtables/" + s.DtableUUID + "/columns/"

	data := make(map[string]interface{})
	data["table_name"] = tableName
	data["column_key"] = columnKey

	jsonStr, err := json.Marshal(data)
	if err != nil {
		err := fmt.Errorf("failed to encode json data: %v", err)
		return nil, err
	}

	status, body, err := httpDelete(url, s.Headers, bytes.NewBuffer(jsonStr), s.Timeout)
	if err != nil {
		err := fmt.Errorf("failed to post row to %s: %v", url, err)
		return nil, err
	}

	if status >= 400 {
		err := fmt.Errorf("bad response for POST: %d", status)
		return nil, err
	}

	rsp, err := parseResponse(body)
	if err != nil {
		err := fmt.Errorf("failed to parse response: %v", err)
		return nil, err
	}

	ret, ok := rsp.(map[string]interface{})
	if !ok {
		err := fmt.Errorf("failed to assert response")
		return nil, err
	}

	return ret, nil
}

func (s *Base) DownloadFile(url, savePath string) error {
	if strings.Index(url, s.DtableUUID) < 0 {
		err := fmt.Errorf("url invalid")
		return err
	}

	paths := strings.Split(url, s.DtableUUID)
	path := strings.Trim(paths[len(paths)-1], "/")

	unescapePath, err := neturl.PathUnescape(path)
	if err != nil {
		return err
	}

	downloadLink, err := s.GetFileDownloadLink(unescapePath)
	if err != nil {
		return err
	}

	url, ok := downloadLink.(string)
	if !ok {
		err := fmt.Errorf("failed to assert download link")
		return err
	}
	status, body, err := httpGet(url, "", s.Headers, s.Timeout, nil)
	if err != nil {
		err := fmt.Errorf("failed to request url: %s: %v", url, err)
		return err
	}

	if status != 200 {
		err := fmt.Errorf("download file error")
		return err
	}

	f, err := os.OpenFile(savePath, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		err := fmt.Errorf("failed to open file %s: %v", savePath, err)
		return err
	}
	defer f.Close()

	_, err = f.Write(body)
	if err != nil {
		err := fmt.Errorf("failed to write file: %v", err)
		return err
	}

	return nil
}

func (s *Base) UploadBytesFile(name string, r io.Reader, relativePath, fileType string, replace bool) (map[string]interface{}, error) {
	uploadLinkDict, err := s.GetFileUploadLink()
	if err != nil {
		err := fmt.Errorf("failed to get file upload link: %v", err)
		return nil, err
	}

	parentDir, _ := uploadLinkDict["parent_path"].(string)
	uploadLink, _ := uploadLinkDict["upload_link"].(string)
	uploadLink = uploadLink + "?ret-json=1"

	if relativePath == "" {
		if fileType != "" && fileType != "image" && fileType != "file" {
			err := fmt.Errorf("relative or file_type invalid")
			return nil, err
		}
		if fileType == "" {
			fileType = "file"
		}
		relativePath = fmt.Sprintf("%ss/%s", fileType, time.Now().Format("2006-01"))
	} else {
		relativePath = strings.Trim(relativePath, "/")
	}

	values := make(map[string]io.Reader)
	values["file"] = r
	values["parent_dir"] = bytes.NewBuffer([]byte(parentDir))
	values["relative_path"] = bytes.NewBuffer([]byte(relativePath))
	if replace {
		values["replace"] = bytes.NewBuffer([]byte("1"))
	} else {
		values["replace"] = bytes.NewBuffer([]byte("0"))
	}
	form, contentType, err := createForm(values, name)
	if err != nil {
		err := fmt.Errorf("failed to create multipart form: %v", err)
		return nil, err
	}

	headers := make(map[string]string)
	for k, v := range s.Headers {
		headers[k] = v
	}
	headers["Content-Type"] = contentType

	status, body, err := httpPost(uploadLink, headers, form, s.Timeout)
	if err != nil {
		err := fmt.Errorf("failed to post row to %s: %v", uploadLink, err)
		return nil, err
	}

	if status >= 400 {
		err := fmt.Errorf("bad response for POST: %d", status)
		return nil, err
	}

	rsp, err := parseResponse(body)
	if err != nil {
		err := fmt.Errorf("failed to parse response: %v", err)
		return nil, err
	}

	datas, ok := rsp.([]interface{})
	if !ok {
		err := fmt.Errorf("failed to assert response")
		return nil, err
	}

	if len(datas) < 1 {
		err := fmt.Errorf("invalid response")
		return nil, err
	}

	data, ok := datas[0].(map[string]interface{})
	if !ok {
		err := fmt.Errorf("failed to assert response")
		return nil, err
	}

	path, err := neturl.PathUnescape(strings.Trim(relativePath, "/"))
	if err != nil {
		return nil, err
	}

	dataname, ok := data["name"].(string)
	if !ok {
		err := fmt.Errorf("failed to assert name")
		return nil, err
	}
	rowName, err := neturl.PathUnescape(strings.Trim(dataname, "/"))
	if err != nil {
		return nil, err
	}

	url := fmt.Sprintf("%s/workspace/%s/asset/%s/%s/%s",
		strings.Trim(s.ServerURL, "/"), s.WorkspaceID,
		s.DtableUUID, path, rowName)

	ret := make(map[string]interface{})
	ret["type"] = fileType
	ret["size"] = data["size"]
	ret["name"] = data["name"]
	ret["url"] = url

	return ret, nil
}

func (s *Base) UploadLocalFile(filePath, name, relativePath, fileType string, replace bool) (map[string]interface{}, error) {
	if fileType != "image" && fileType != "file" {
		err := fmt.Errorf("file_type invalid")
		return nil, err
	}

	if name == "" {
		filePath = strings.Trim(filePath, "/")
		paths := strings.Split(filePath, "/")
		name = paths[len(paths)-1]
	}

	if relativePath == "" {
		if fileType != "" && fileType != "image" && fileType != "file" {
			err := fmt.Errorf("relative or file_type invalid")
			return nil, err
		}
		if fileType == "" {
			fileType = "file"
		}
		relativePath = fmt.Sprintf("%ss/%s", fileType, time.Now().Format("2006-01"))
	} else {
		relativePath = strings.Trim(relativePath, "/")
	}

	uploadLinkDict, err := s.GetFileUploadLink()
	if err != nil {
		err := fmt.Errorf("failed to get file upload link: %v", err)
		return nil, err
	}

	parentDir, _ := uploadLinkDict["parent_path"].(string)
	uploadLink, _ := uploadLinkDict["upload_link"].(string)
	uploadLink = uploadLink + "?ret-json=1"

	values := make(map[string]io.Reader)
	values["parent_dir"] = bytes.NewBuffer([]byte(parentDir))
	values["relative_path"] = bytes.NewBuffer([]byte(relativePath))
	if replace {
		values["replace"] = bytes.NewBuffer([]byte("1"))
	} else {
		values["replace"] = bytes.NewBuffer([]byte("0"))
	}
	f, err := os.Open(filePath)
	if err != nil {
		err := fmt.Errorf("failed to open local file: %v", err)
		return nil, err
	}
	defer f.Close()
	values["file"] = f

	form, contentType, err := createForm(values, name)
	if err != nil {
		err := fmt.Errorf("failed to create multipart form: %v", err)
		return nil, err
	}

	headers := make(map[string]string)
	for k, v := range s.Headers {
		headers[k] = v
	}
	headers["Content-Type"] = contentType

	status, body, err := httpPost(uploadLink, headers, form, s.Timeout)
	if err != nil {
		err := fmt.Errorf("failed to post row to %s: %v", uploadLink, err)
		return nil, err
	}

	if status >= 400 {
		err := fmt.Errorf("bad response for POST: %d", status)
		return nil, err
	}

	rsp, err := parseResponse(body)
	if err != nil {
		err := fmt.Errorf("failed to parse response: %v", err)
		return nil, err
	}

	datas, ok := rsp.([]interface{})
	if !ok {
		err := fmt.Errorf("failed to assert response")
		return nil, err
	}

	if len(datas) < 1 {
		err := fmt.Errorf("invalid response")
		return nil, err
	}

	data, ok := datas[0].(map[string]interface{})
	if !ok {
		err := fmt.Errorf("failed to assert response")
		return nil, err
	}

	path, err := neturl.PathUnescape(strings.Trim(relativePath, "/"))
	if err != nil {
		return nil, err
	}

	dataname, ok := data["name"].(string)
	if !ok {
		err := fmt.Errorf("failed to assert name")
		return nil, err
	}
	rowName, err := neturl.PathUnescape(strings.Trim(dataname, "/"))
	if err != nil {
		return nil, err
	}

	url := fmt.Sprintf("%s/workspace/%s/asset/%s/%s/%s",
		strings.Trim(s.ServerURL, "/"), s.WorkspaceID,
		s.DtableUUID, path, rowName)

	ret := make(map[string]interface{})
	ret["type"] = fileType
	ret["size"] = data["size"]
	ret["name"] = data["name"]
	ret["url"] = url

	return ret, nil
}

/*
func (s *Base) Filter(tableName, conditions, viewName string) (*QuerySet, error) {
	var err error
	base, err := s.Clone()
	if err != nil {
		err := fmt.Errorf("failed to clone base: %v", err)
		return nil, err
	}
	queryset := NewQuerySet(base, tableName)
	queryset.RawRows, err = s.ListRows(tableName, viewName)
	if err != nil {
		return nil, err
	}
	queryset.RawColumns, err = s.ListColumns(tableName, viewName)
	if err != nil {
		return nil, err
	}
	queryset.Conditions = conditions
	queryset.ExecuteConditions()

	return queryset, nil
}
*/

func (s *Base) ListRows(tableName, viewName string) (interface{}, error) {
	url := s.DtableServerURL + "/api/v1/dtables/" + s.DtableUUID + "/rows/"

	params := neturl.Values{}
	params.Add("table_name", tableName)
	if viewName != "" {
		params.Add("view_name", viewName)
	}

	status, body, err := httpGet(url, params.Encode(), s.Headers, s.Timeout, nil)
	if err != nil {
		err := fmt.Errorf("failed to request url: %s: %v", url, err)
		return nil, err
	}

	if status >= 400 {
		err := fmt.Errorf("bad response for GET: %d", status)
		return nil, err
	}

	rsp, err := parseResponse(body)
	if err != nil {
		err := fmt.Errorf("failed to parse response: %v", err)
		return nil, err
	}

	ret, ok := rsp.(map[string]interface{})
	if !ok {
		err := fmt.Errorf("failed to assert response")
		return nil, err
	}

	return ret["rows"], nil
}

func createForm(values map[string]io.Reader, name string) (io.Reader, string, error) {
	buf := new(bytes.Buffer)
	w := multipart.NewWriter(buf)
	defer w.Close()

	for k, v := range values {
		var fw io.Writer
		var err error
		if k == "file" {
			if fw, err = w.CreateFormFile(k, name); err != nil {
				return nil, "", err
			}
		} else {
			if fw, err = w.CreateFormField(k); err != nil {
				return nil, "", err
			}
		}
		if _, err = io.Copy(fw, v); err != nil {
			return nil, "", err
		}
	}

	return buf, w.FormDataContentType(), nil
}

func makeHeaders(token string) map[string]string {
	headers := make(map[string]string)
	headers["Content-Type"] = "application/json"
	headers["Authorization"] = "Token " + token

	return headers
}

func parseResponse(body []byte) (interface{}, error) {
	var data interface{}
	//data := make(map[string]interface{})
	err := json.Unmarshal(body, &data)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func httpGet(url, params string, headers map[string]string, timeout int, body io.Reader) (int, []byte, error) {
	req, err := http.NewRequest("GET", url, body)
	if err != nil {
		err := fmt.Errorf("failed to create http GET request: %v", err)
		return 0, nil, err
	}

	if params != "" {
		req.URL.RawQuery = params
	}

	return httpCommon(req, headers, timeout)
}

func httpPost(url string, headers map[string]string, body io.Reader, timeout int) (int, []byte, error) {
	req, err := http.NewRequest("POST", url, body)
	if err != nil {
		err := fmt.Errorf("failed to create http POST request: %v", err)
		return 0, nil, err
	}

	return httpCommon(req, headers, timeout)
}

func httpPut(url string, headers map[string]string, body io.Reader, timeout int) (int, []byte, error) {
	req, err := http.NewRequest("PUT", url, body)
	if err != nil {
		err := fmt.Errorf("failed to create http POST request: %v", err)
		return 0, nil, err
	}

	return httpCommon(req, headers, timeout)
}

func httpDelete(url string, headers map[string]string, body io.Reader, timeout int) (int, []byte, error) {
	req, err := http.NewRequest("DELETE", url, body)
	if err != nil {
		err := fmt.Errorf("failed to create http POST request: %v", err)
		return 0, nil, err
	}

	return httpCommon(req, headers, timeout)
}

func httpCommon(req *http.Request, headers map[string]string, timeout int) (int, []byte, error) {
	for k, v := range headers {
		req.Header.Add(k, v)
	}

	var client *http.Client
	if timeout > 0 {
		client = &http.Client{Timeout: time.Duration(timeout) * time.Second}
	} else {
		client = &http.Client{}
	}

	rsp, err := client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer rsp.Body.Close()

	body, err := ioutil.ReadAll(rsp.Body)
	if err != nil {
		err := fmt.Errorf("failed to read from response body: %v", err)
		return rsp.StatusCode, nil, err
	}

	return rsp.StatusCode, body, nil
}
