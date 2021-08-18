package es

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/moul/http2curl"
	"github.com/rerost/es-cli/config"
	"github.com/srvc/fail"
	"go.uber.org/zap"
)

type Index struct {
	Name string
}
type Indices []Index

func (i Index) String() string {
	return i.Name
}

func (is Indices) String() string {
	result := make([]string, len(is), len(is))
	for i, index := range is {
		result[i] = index.String()
	}

	return strings.Join(result, "\n")
}

type Opt struct{}
type Alias struct{}
type Task struct {
	ID       string
	Complete bool
}

func (t Task) String() string {
	return fmt.Sprintf("ID: %s, Complete: %v", t.ID, t.Complete)
}

type Tasks []Task

func (ts Tasks) String() string {
	result := make([]string, len(ts), len(ts))
	for i, t := range ts {
		result[i] = t.String()
	}

	return strings.Join(result, "\n")
}

type Count struct {
	Num int64
}

func (c Count) String() string {
	return fmt.Sprintf("%d", c.Num)
}

type Version struct {
	Number string `json:"number"`
}

type IndexDetail struct {
	Setting interface{} `json:"settings"`
	Alias   interface{} `json:"aliases"`
	Mapping interface{} `json:"mappings"`
}

func (i IndexDetail) String() string {
	body, err := json.Marshal(i)
	if err != nil {
		panic(err)
	}
	return string(body)
}

func (c Version) String() string {
	return c.Number
}

type Pong struct {
	OK bool
}

func (c Pong) String() string {
	if c.OK {
		return "Pong"
	}
	return "Failed"
}

type SearchResponse struct {
	Hits struct {
		Total int64 `json:"total"`
		Hits  []struct {
			ID     string                 `json:"_id"`
			Type   string                 `json:"_type"`
			Index  string                 `json:"_index"`
			Source map[string]interface{} `json:"_source"`
		} `json:"hits"`
	} `json:"hits"`
}

func (r SearchResponse) String() string {
	b, err := json.Marshal(r)
	if err != nil {
		panic(err)
	}
	return string(b)
}

// Client is http wrapper
type BaseClient interface {
	// Index
	ListIndex(ctx context.Context) (Indices, error)
	CreateIndex(ctx context.Context, indexName string, mappingJSON string) error
	CopyIndex(ctx context.Context, srcIndexName string, dstIndexName string) (Task, error)
	DeleteIndex(ctx context.Context, indexName string) error
	CountIndex(ctx context.Context, indexName string) (Count, error)
	SearchIndex(ctx context.Context, indexName string, query string) (SearchResponse, error)
	BulkIndex(ctx context.Context, body string) error

	// Detail
	DetailIndex(ctx context.Context, indexName string) (IndexDetail, error)

	// Alias
	AddAlias(ctx context.Context, aliasName string, indexNames ...string) error
	RemoveAlias(ctx context.Context, aliasName string, indexNames ...string) error
	ListAlias(ctx context.Context, aliasName string) (Indices, error)
	SwapAlias(ctx context.Context, aliasName string, removeIndexName string, addIndexName string) error

	// Task
	GetTask(ctx context.Context, taskID string) (Task, error)

	Version(ctx context.Context) (Version, error)
	Ping(ctx context.Context) (Pong, error)
}

type baseClientImp struct {
	Config     config.Config
	HttpClient *http.Client
}

func NewBaseClient(cfg config.Config, httpClient *http.Client) (BaseClient, error) {
	client := baseClientImp{}
	client.HttpClient = httpClient
	client.Config = cfg

	return client, nil
}

func (client baseClientImp) httpRequest(ctx context.Context, method string, url string, body string, contentType string, params map[string]string) ([]byte, error) {
	request, err := http.NewRequest(method, url, bytes.NewBufferString(body))
	if err != nil {
		return nil, fail.Wrap(err)
	}

	if contentType != "" {
		request.Header.Add("Content-Type", contentType)
	}

	if params != nil {
		request = addParams(request, params)
	}

	if client.Config.User != "" && client.Config.Pass != "" {
		request.SetBasicAuth(client.Config.User, client.Config.Pass)
	}

	// Request log
	{
		c, err := http2curl.GetCurlCommand(request)
		if err != nil {
			zap.L().Debug(
				"Failed to convert to curl",
				zap.Error(err),
			)
		}
		zap.L().Debug(
			"request",
			zap.String("curl", c.String()),
		)
	}
	response, err := client.HttpClient.Do(request)
	if err != nil {
		return nil, fail.Wrap(err)
	}
	responseMap := map[string]interface{}{}

	responseBody, err := ioutil.ReadAll(response.Body)
	err = json.Unmarshal(responseBody, &responseMap)
	if err != nil {
		return nil, fail.Wrap(err)
	}
	defer response.Body.Close()

	// Response log
	{
		zap.L().Debug(
			"response",
			zap.String("response status", string(response.Status)),
			zap.String("response body", string(responseBody)),
		)
	}

	if errMsg, ok := responseMap["error"]; ok {
		return nil, fail.New(fmt.Sprintf("%v", errMsg))
	}

	if errMsg, ok := responseMap["errors"]; ok {
		return nil, fail.New(fmt.Sprintf("%v", errMsg))
	}

	return responseBody, nil
}

func (client baseClientImp) ListIndex(ctx context.Context) (Indices, error) {
	indices := Indices{}

	responseBody, err := client.httpRequest(ctx, http.MethodGet, client.listIndexURL(), "", "", nil)
	if err != nil {
		return indices, fail.Wrap(err)
	}

	responseMap := map[string]interface{}{}
	err = json.Unmarshal(responseBody, &responseMap)
	if err != nil {
		return indices, fail.Wrap(err)
	}

	indices = make(Indices, len(responseMap), len(responseMap))
	i := 0
	for indexName := range responseMap {
		indices[i] = Index{Name: indexName}
		i++
	}
	return indices, nil
}
func (client baseClientImp) CreateIndex(ctx context.Context, indexName string, mappingJSON string) error {
	params := map[string]string{}
	if client.Config.SetIncludeTypeName {
		params["include_type_name"] = "true"
	}

	responseBody, err := client.httpRequest(ctx, http.MethodPut, client.rawIndexURL(indexName), mappingJSON, "application/json", params)
	if err != nil {
		return fail.Wrap(err)
	}

	responseMap := map[string]interface{}{}
	err = json.Unmarshal(responseBody, &responseMap)
	if err != nil {
		return fail.Wrap(err)
	}

	return nil
}
func (client baseClientImp) CopyIndex(ctx context.Context, srcIndexName string, dstIndexName string) (Task, error) {
	reindexJSON := fmt.Sprintf(`
{
	"source": {
		"index": "%s"
	},
	"dest": {
		"index": "%s"
	}
}
	`, srcIndexName, dstIndexName)
	responseBody, err := client.httpRequest(ctx, http.MethodPost, client.reindexURL(), reindexJSON, "application/json", map[string]string{"wait_for_completion": "false"})
	if err != nil {
		return Task{}, fail.Wrap(err)
	}

	responseMap := map[string]interface{}{}
	err = json.Unmarshal(responseBody, &responseMap)
	if err != nil {
		return Task{}, fail.Wrap(err)
	}

	if _, ok := responseMap["task"].(string); !ok {
		return Task{}, fail.New(fmt.Sprintf("Not found task: %v", string(responseBody)))
	}

	taskID := responseMap["task"].(string)

	return Task{ID: taskID}, nil
}
func (client baseClientImp) DeleteIndex(ctx context.Context, indexName string) error {
	responseBody, err := client.httpRequest(ctx, http.MethodDelete, client.rawIndexURL(indexName), "", "", nil)
	if err != nil {
		return fail.Wrap(err)
	}

	responseMap := map[string]interface{}{}
	err = json.Unmarshal(responseBody, &responseMap)
	if err != nil {
		return fail.Wrap(err)
	}

	return nil
}
func (client baseClientImp) CountIndex(ctx context.Context, indexName string) (Count, error) {
	responseBody, err := client.httpRequest(ctx, http.MethodGet, client.countURL(indexName), "", "", nil)
	if err != nil {
		return Count{}, fail.Wrap(err)
	}

	responseMap := map[string]interface{}{}
	err = json.Unmarshal(responseBody, &responseMap)
	if err != nil {
		return Count{}, fail.Wrap(err)
	}

	if _, ok := responseMap["count"].(float64); !ok {
		return Count{}, fail.New(fmt.Sprintf("Failed to extract count from json: %s", responseBody))
	}

	return Count{Num: int64(responseMap["count"].(float64))}, nil
}
func (client baseClientImp) SearchIndex(ctx context.Context, indexName string, query string) (SearchResponse, error) {
	responseBody, err := client.httpRequest(ctx, http.MethodPost, client.searchURL(indexName), query, "application/json", nil)
	if err != nil {
		return SearchResponse{}, fail.Wrap(err)
	}

	searchResponse := SearchResponse{}
	err = json.Unmarshal(responseBody, &searchResponse)
	if err != nil {
		return SearchResponse{}, fail.Wrap(err)
	}

	return searchResponse, nil
}
func (client baseClientImp) BulkIndex(ctx context.Context, body string) error {
	responseBody, err := client.httpRequest(ctx, http.MethodPost, client.bulkURL(), body, "application/x-ndjson", nil)
	if err != nil {
		return fail.Wrap(err)
	}

	responseMap := map[string]interface{}{}

	err = json.Unmarshal(responseBody, &responseMap)
	if err != nil {
		return fail.Wrap(err)
	}

	return nil
}

func (client baseClientImp) DetailIndex(ctx context.Context, indexName string) (IndexDetail, error) {
	responseBody, err := client.httpRequest(ctx, http.MethodGet, client.detailURL(indexName), "", "", nil)
	indexDetail := IndexDetail{}
	if err != nil {
		return indexDetail, fail.Wrap(err)
	}

	responseMap := map[string]interface{}{}

	err = json.Unmarshal(responseBody, &responseMap)
	if err != nil {
		return indexDetail, fail.Wrap(err)
	}

	detail := responseMap[indexName]
	jdetail, err := json.Marshal(detail)
	if err != nil {
		return indexDetail, fail.Wrap(err)
	}

	detailMap := map[string]interface{}{}
	err = json.Unmarshal(jdetail, &detailMap)
	if err != nil {
		return indexDetail, fail.Wrap(err)
	}

	indexDetail.Mapping = detailMap["mappings"]
	indexDetail.Setting = detailMap["settings"]
	indexDetail.Alias = detailMap["aliases"]
	return indexDetail, nil
}

// Alias
func (client baseClientImp) AddAlias(ctx context.Context, aliasName string, indexNames ...string) error {
	actions := []string{}
	for _, indexName := range indexNames {
		actions = append(actions, fmt.Sprintf(`
{
	"add": {
		"index": "%s",
		"alias": "%s"
	}
}
		`, indexName, aliasName))
	}

	addAliasJSON := fmt.Sprintf(`
{
	"actions": [
		%s
	]
}`, strings.Join(actions, ",\n"))

	responseBody, err := client.httpRequest(ctx, http.MethodPost, client.aliasURL(), addAliasJSON, "application/json", nil)
	if err != nil {
		return fail.Wrap(err)
	}

	responseMap := map[string]interface{}{}
	err = json.Unmarshal(responseBody, &responseMap)
	if err != nil {
		return fail.Wrap(err)
	}

	return nil
}
func (client baseClientImp) RemoveAlias(ctx context.Context, aliasName string, indexNames ...string) error {
	actions := []string{}
	for _, indexName := range indexNames {
		actions = append(actions, fmt.Sprintf(`
{
	"remove": {
		"index": "%s",
		"alias": "%s"
	}
}
		`, indexName, aliasName))
	}

	removeAliasJSON := fmt.Sprintf(`
{
	"actions": [
		%s
	]
}`, strings.Join(actions, ",\n"))

	responseBody, err := client.httpRequest(ctx, http.MethodPost, client.aliasURL(), removeAliasJSON, "application/json", nil)
	if err != nil {
		return fail.Wrap(err)
	}

	responseMap := map[string]interface{}{}

	err = json.Unmarshal(responseBody, &responseMap)
	if err != nil {
		return fail.Wrap(err)
	}

	return nil
}
func (client baseClientImp) SwapAlias(ctx context.Context, aliasName string, removeIndexName string, addIndexName string) error {
	swapAliasJSON := fmt.Sprintf(`
{
	"actions": [
		{
			"remove": {
				"index": "%s",
				"alias": "%s"
			}
		},
		{
			"add": {
				"index": "%s",
				"alias": "%s"
			}
		}
	]
}`, removeIndexName, aliasName, addIndexName, aliasName)

	responseBody, err := client.httpRequest(ctx, http.MethodPost, client.aliasURL(), swapAliasJSON, "application/json", nil)
	if err != nil {
		return fail.Wrap(err)
	}

	responseMap := map[string]interface{}{}

	err = json.Unmarshal(responseBody, &responseMap)
	if err != nil {
		return fail.Wrap(err)
	}

	return nil
}
func (client baseClientImp) ListAlias(ctx context.Context, aliasName string) (Indices, error) {
	indices := Indices{}
	responseBody, err := client.httpRequest(ctx, http.MethodGet, client.rawIndexURL(aliasName), "", "", nil)
	if err != nil {
		return indices, fail.Wrap(err)
	}

	responseMap := map[string]interface{}{}

	err = json.Unmarshal(responseBody, &responseMap)
	if err != nil {
		return indices, fail.Wrap(err)
	}

	indices = make(Indices, len(responseMap), len(responseMap))
	i := 0
	for indexName := range responseMap {
		indices[i] = Index{Name: indexName}
		i++
	}
	return indices, nil
}

// Task
func (client baseClientImp) GetTask(ctx context.Context, taskID string) (Task, error) {
	responseBody, err := client.httpRequest(ctx, http.MethodGet, client.taskURL(taskID), "", "", nil)
	if err != nil {
		return Task{}, fail.Wrap(err)
	}

	responseMap := map[string]interface{}{}

	err = json.Unmarshal(responseBody, &responseMap)
	if err != nil {
		return Task{}, fail.Wrap(err)
	}

	if _, ok := responseMap["completed"].(bool); !ok {
		return Task{}, fail.New(fmt.Sprintf("Failed to extract completed from resposne"))
	}

	return Task{Complete: responseMap["completed"].(bool)}, nil
}

// Util
func (client baseClientImp) Version(ctx context.Context) (Version, error) {
	responseBody, err := client.httpRequest(ctx, http.MethodGet, client.baseURL(), "", "application/json", nil)
	if err != nil {
		return Version{}, fail.Wrap(err)
	}

	responseMap := map[string]interface{}{}

	err = json.Unmarshal(responseBody, &responseMap)
	if err != nil {
		return Version{}, fail.Wrap(err)
	}

	jsonVersion, err := json.Marshal(responseMap["version"])
	if err != nil {
		return Version{}, fail.Wrap(err)
	}
	version := Version{}
	err = json.Unmarshal(jsonVersion, &version)

	if err != nil {
		return Version{}, fail.Wrap(err)
	}
	if version.Number == "" {
		return Version{}, fail.New(fmt.Sprintf("Invalid response is returned %v", string(responseBody)))
	}

	return version, nil
}
func (client baseClientImp) Ping(ctx context.Context) (Pong, error) {
	request, err := http.NewRequest(http.MethodGet, client.baseURL(), bytes.NewBufferString(""))
	if err != nil {
		return Pong{OK: false}, fail.Wrap(err)
	}

	if client.Config.User != "" && client.Config.Pass != "" {
		request.SetBasicAuth(client.Config.User, client.Config.Pass)
	}

	response, err := client.HttpClient.Do(request)
	if err != nil {
		return Pong{OK: false}, fail.Wrap(err)
	}
	defer response.Body.Close()

	if response.StatusCode != 200 {
		responseBody, _ := ioutil.ReadAll(response.Body)
		return Pong{OK: false}, fail.New(string(responseBody))
	}

	return Pong{OK: true}, nil
}

func (client baseClientImp) baseURL() string {
	return client.Config.Host
}
func (client baseClientImp) listIndexURL() string {
	return client.baseURL() + "/_aliases"
}
func (client baseClientImp) indexURL(indexName string) string {
	return client.baseURL() + "/" + indexName + "/" + client.Config.Type
}
func (client baseClientImp) rawIndexURL(indexName string) string {
	return client.baseURL() + "/" + indexName
}
func (client baseClientImp) reindexURL() string {
	return client.baseURL() + "/_reindex"
}
func (client baseClientImp) tasksURL() string {
	return client.baseURL() + "/_tasks"
}
func (client baseClientImp) taskURL(taskID string) string {
	return client.tasksURL() + "/" + taskID
}
func (client baseClientImp) mappingURL(indexOrAliasName string) string {
	return client.baseURL() + "/" + indexOrAliasName + "/" + "_mapping" + "/" + client.Config.Type
}
func (client baseClientImp) aliasURL() string {
	return client.baseURL() + "/_aliases"
}
func (client baseClientImp) countURL(indexName string) string {
	return client.indexURL(indexName) + "/_count"
}
func (client baseClientImp) searchURL(indexName string) string {
	return client.baseURL() + "/" + indexName + "/_search"
}
func (client baseClientImp) bulkURL() string {
	return client.baseURL() + "/_bulk"
}
func (client baseClientImp) detailURL(indexName string) string {
	return client.baseURL() + "/" + indexName
}

func addParams(req *http.Request, params map[string]string) *http.Request {
	q := req.URL.Query()
	for k, v := range params {
		q.Add(k, v)
	}

	req.URL.RawQuery = q.Encode()
	return req
}
