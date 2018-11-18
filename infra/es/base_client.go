package es

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/rerost/es-cli/setting"
	"github.com/srvc/fail"
	"gopkg.in/guregu/null.v3"
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

type Mapping string

func (m Mapping) String() string {
	return string(m)
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
	DetailIndex(ctx context.Context, indexName string) (IndexDetail, error)

	// Mapping
	GetMapping(ctx context.Context, indexOrAliasName string) (Mapping, error)
	// UpdateMapping(ctx context.Context, aliasName string, mappingJSON string) error

	// Alias
	AddAlias(ctx context.Context, aliasName string, indexNames ...string) error
	RemoveAlias(ctx context.Context, aliasName string, indexNames ...string) error
	ListAlias(ctx context.Context, aliasName string) (Indices, error)

	// Task
	ListTask(ctx context.Context) (Tasks, error)
	GetTask(ctx context.Context, taskID string) (Task, error)

	Version(ctx context.Context) (Version, error)
	Ping(ctx context.Context) (Pong, error)
}

type baseClientImp struct {
	Host       string
	Port       string
	Type       string
	User       null.String
	Pass       null.String
	HttpClient *http.Client
}

func NewBaseClient(ctx context.Context, httpClient *http.Client) (BaseClient, error) {
	client := baseClientImp{}
	client.HttpClient = httpClient

	_host, ok := ctx.Value(setting.SettingKey("host")).(string)
	if !ok {
		return client, fail.New("Failed to extract host")
	}

	_port, ok := ctx.Value(setting.SettingKey("port")).(string)
	if !ok {
		return client, fail.New("Failed to extract port")
	}

	_type, ok := ctx.Value(setting.SettingKey("type")).(string)
	if !ok {
		return client, fail.New("Failed to extract type")
	}

	client.Host = _host
	client.Port = _port
	client.Type = _type

	_user, ok := ctx.Value(setting.SettingKey("user")).(string)
	if ok {
		client.User = null.StringFrom(_user)
	}

	_pass, ok := ctx.Value(setting.SettingKey("pass")).(string)
	if ok {
		client.Pass = null.StringFrom(_pass)
	}

	return client, nil
}

func (client baseClientImp) ListIndex(ctx context.Context) (Indices, error) {
	indices := Indices{}
	request, err := http.NewRequest(http.MethodGet, client.listIndexURL(), bytes.NewBufferString(""))
	if err != nil {
		return indices, fail.Wrap(err)
	}

	if client.User.Valid && client.Pass.Valid && client.User.String != "" && client.Pass.String != "" {
		request.SetBasicAuth(client.User.String, client.Pass.String)
	}

	response, err := client.HttpClient.Do(request)
	if err != nil {
		return indices, fail.Wrap(err)
	}
	responseMap := map[string]interface{}{}

	responseBody, err := ioutil.ReadAll(response.Body)
	err = json.Unmarshal(responseBody, &responseMap)
	if err != nil {
		return indices, fail.Wrap(err)
	}
	defer response.Body.Close()

	if errMsg, ok := responseMap["error"]; ok {
		return indices, fail.New(fmt.Sprintf("%v", errMsg))
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
	request, err := http.NewRequest(http.MethodPost, client.indexURL(indexName), bytes.NewBufferString(mappingJSON))
	if err != nil {
		return fail.Wrap(err)
	}

	request.Header.Add("Content-Type", "application/json")
	if client.User.Valid && client.Pass.Valid && client.User.String != "" && client.Pass.String != "" {
		request.SetBasicAuth(client.User.String, client.Pass.String)
	}

	response, err := client.HttpClient.Do(request)
	if err != nil {
		return fail.Wrap(err)
	}
	defer response.Body.Close()

	responseMap := map[string]interface{}{}

	responseBody, err := ioutil.ReadAll(response.Body)
	err = json.Unmarshal(responseBody, &responseMap)
	if err != nil {
		return fail.Wrap(err)
	}

	if errMsg, ok := responseMap["error"]; ok {
		return fail.New(fmt.Sprintf("%v", errMsg))
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
	request, err := http.NewRequest(http.MethodPost, client.reindexURL(), bytes.NewBufferString(reindexJSON))
	if err != nil {
		return Task{}, fail.Wrap(err)
	}

	if client.User.Valid && client.Pass.Valid && client.User.String != "" && client.Pass.String != "" {
		request.SetBasicAuth(client.User.String, client.Pass.String)
	}
	request.Header.Add("Content-Type", "application/json")
	request = addParams(request, map[string]string{"wait_for_completion": "false"})

	response, err := client.HttpClient.Do(request)
	if err != nil {
		return Task{}, fail.Wrap(err)
	}
	defer response.Body.Close()

	responseMap := map[string]interface{}{}

	responseBody, err := ioutil.ReadAll(response.Body)
	err = json.Unmarshal(responseBody, &responseMap)
	if err != nil {
		return Task{}, fail.Wrap(err)
	}

	if errMsg, ok := responseMap["error"]; ok {
		return Task{}, fail.New(fmt.Sprintf("%v", errMsg))
	}

	if _, ok := responseMap["task"].(string); !ok {
		return Task{}, fail.New(fmt.Sprintf("Not found task: %v", string(responseBody)))
	}

	taskID := responseMap["task"].(string)

	return Task{ID: taskID}, nil
}
func (client baseClientImp) DeleteIndex(ctx context.Context, indexName string) error {
	request, err := http.NewRequest(http.MethodDelete, client.rawIndexURL(indexName), bytes.NewBufferString(""))
	if err != nil {
		return fail.Wrap(err)
	}

	if client.User.Valid && client.Pass.Valid && client.User.String != "" && client.Pass.String != "" {
		request.SetBasicAuth(client.User.String, client.Pass.String)
	}

	response, err := client.HttpClient.Do(request)
	if err != nil {
		return fail.Wrap(err)
	}
	defer response.Body.Close()

	responseMap := map[string]interface{}{}

	responseBody, err := ioutil.ReadAll(response.Body)
	err = json.Unmarshal(responseBody, &responseMap)
	if err != nil {
		return fail.Wrap(err)
	}

	if errMsg, ok := responseMap["error"]; ok {
		return fail.New(fmt.Sprintf("%v", errMsg))
	}

	return nil
}
func (client baseClientImp) CountIndex(ctx context.Context, indexName string) (Count, error) {
	request, err := http.NewRequest(http.MethodGet, client.countURL(indexName), bytes.NewBufferString(""))
	if err != nil {
		return Count{}, fail.Wrap(err)
	}

	if client.User.Valid && client.Pass.Valid && client.User.String != "" && client.Pass.String != "" {
		request.SetBasicAuth(client.User.String, client.Pass.String)
	}

	response, err := client.HttpClient.Do(request)
	if err != nil {
		return Count{}, fail.Wrap(err)
	}
	defer response.Body.Close()

	responseMap := map[string]interface{}{}

	responseBody, err := ioutil.ReadAll(response.Body)
	err = json.Unmarshal(responseBody, &responseMap)
	if err != nil {
		return Count{}, fail.Wrap(err)
	}

	if errMsg, ok := responseMap["error"]; ok {
		return Count{}, fail.New(fmt.Sprintf("%v", errMsg))
	}

	if _, ok := responseMap["count"].(float64); !ok {
		return Count{}, fail.New(fmt.Sprintf("Failed to extract count from json: %s", responseBody))
	}

	return Count{Num: int64(responseMap["count"].(float64))}, nil
}
func (client baseClientImp) SearchIndex(ctx context.Context, indexName string, query string) (SearchResponse, error) {
	request, err := http.NewRequest(http.MethodPost, client.searchURL(indexName), bytes.NewBufferString(query))
	if err != nil {
		return SearchResponse{}, fail.Wrap(err)
	}

	request.Header.Set("Content-Type", "application/json")
	if client.User.Valid && client.Pass.Valid && client.User.String != "" && client.Pass.String != "" {
		request.SetBasicAuth(client.User.String, client.Pass.String)
	}

	response, err := client.HttpClient.Do(request)
	if err != nil {
		return SearchResponse{}, fail.Wrap(err)
	}
	defer response.Body.Close()

	responseMap := map[string]interface{}{}

	responseBody, err := ioutil.ReadAll(response.Body)
	err = json.Unmarshal(responseBody, &responseMap)
	if err != nil {
		return SearchResponse{}, fail.Wrap(err)
	}

	if errMsg, ok := responseMap["error"]; ok {
		return SearchResponse{}, fail.New(fmt.Sprintf("%v", errMsg))
	}

	searchResponse := SearchResponse{}
	err = json.Unmarshal(responseBody, &searchResponse)
	if err != nil {
		return SearchResponse{}, fail.Wrap(err)
	}

	return searchResponse, nil
}
func (client baseClientImp) BulkIndex(ctx context.Context, body string) error {
	request, err := http.NewRequest(http.MethodPost, client.bulkURL(), bytes.NewBufferString(body))
	if err != nil {
		return fail.Wrap(err)
	}

	request.Header.Add("Content-Type", "application/x-ndjson")
	if client.User.Valid && client.Pass.Valid && client.User.String != "" && client.Pass.String != "" {
		request.SetBasicAuth(client.User.String, client.Pass.String)
	}

	response, err := client.HttpClient.Do(request)
	if err != nil {
		return fail.Wrap(err)
	}
	defer response.Body.Close()

	responseMap := map[string]interface{}{}

	responseBody, err := ioutil.ReadAll(response.Body)
	err = json.Unmarshal(responseBody, &responseMap)
	if err != nil {
		return fail.Wrap(err)
	}

	if errMsg, ok := responseMap["error"]; ok {
		return fail.New(fmt.Sprintf("%v", errMsg))
	}
	if errMsg, ok := responseMap["errors"]; ok {
		// BulkIndex return false when error nil
		if errBool := responseMap["errors"].(bool); !errBool {
			return nil
		}
		return fail.New(fmt.Sprintf("%v", errMsg))
	}

	return nil
}

func (client baseClientImp) DetailIndex(ctx context.Context, indexName string) (IndexDetail, error) {
	request, err := http.NewRequest(http.MethodGet, client.detailURL(indexName), bytes.NewBufferString(""))
	indexDetail := IndexDetail{}
	if err != nil {
		return indexDetail, fail.Wrap(err)
	}

	if client.User.Valid && client.Pass.Valid && client.User.String != "" && client.Pass.String != "" {
		request.SetBasicAuth(client.User.String, client.Pass.String)
	}

	response, err := client.HttpClient.Do(request)
	if err != nil {
		return indexDetail, fail.Wrap(err)
	}
	defer response.Body.Close()

	responseMap := map[string]interface{}{}

	responseBody, err := ioutil.ReadAll(response.Body)
	err = json.Unmarshal(responseBody, &responseMap)
	if err != nil {
		return indexDetail, fail.Wrap(err)
	}

	if errMsg, ok := responseMap["error"]; ok {
		return indexDetail, fail.New(fmt.Sprintf("%v", errMsg))
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

// Mapping
func (client baseClientImp) GetMapping(ctx context.Context, indexOrAliasName string) (Mapping, error) {
	request, err := http.NewRequest(http.MethodGet, client.mappingURL(indexOrAliasName), bytes.NewBufferString(""))
	if err != nil {
		return Mapping(""), fail.Wrap(err)
	}

	if client.User.Valid && client.Pass.Valid && client.User.String != "" && client.Pass.String != "" {
		request.SetBasicAuth(client.User.String, client.Pass.String)
	}

	response, err := client.HttpClient.Do(request)
	if err != nil {
		return Mapping(""), fail.Wrap(err)
	}
	defer response.Body.Close()

	responseMap := map[string]interface{}{}

	responseBody, err := ioutil.ReadAll(response.Body)
	err = json.Unmarshal(responseBody, &responseMap)
	if err != nil {
		return Mapping(""), fail.Wrap(err)
	}

	if errMsg, ok := responseMap["error"]; ok {
		return Mapping(""), fail.New(fmt.Sprintf("%v", errMsg))
	}

	return Mapping(string(responseBody)), nil
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

	request, err := http.NewRequest(http.MethodPost, client.aliasURL(), bytes.NewBufferString(addAliasJSON))
	request.Header.Add("Content-Type", "application/json")
	if err != nil {
		return fail.Wrap(err)
	}

	if client.User.Valid && client.Pass.Valid && client.User.String != "" && client.Pass.String != "" {
		request.SetBasicAuth(client.User.String, client.Pass.String)
	}

	response, err := client.HttpClient.Do(request)
	if err != nil {
		return fail.Wrap(err)
	}
	defer response.Body.Close()

	responseMap := map[string]interface{}{}

	responseBody, err := ioutil.ReadAll(response.Body)
	err = json.Unmarshal(responseBody, &responseMap)
	if err != nil {
		return fail.Wrap(err)
	}

	if errMsg, ok := responseMap["error"]; ok {
		return fail.New(fmt.Sprintf("%v", errMsg))
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

	request, err := http.NewRequest(http.MethodPost, client.aliasURL(), bytes.NewBufferString(removeAliasJSON))
	request.Header.Add("Content-Type", "application/json")
	if err != nil {
		return fail.Wrap(err)
	}

	if client.User.Valid && client.Pass.Valid && client.User.String != "" && client.Pass.String != "" {
		request.SetBasicAuth(client.User.String, client.Pass.String)
	}

	response, err := client.HttpClient.Do(request)
	if err != nil {
		return fail.Wrap(err)
	}
	defer response.Body.Close()

	responseMap := map[string]interface{}{}

	responseBody, err := ioutil.ReadAll(response.Body)
	err = json.Unmarshal(responseBody, &responseMap)
	if err != nil {
		return fail.Wrap(err)
	}

	if errMsg, ok := responseMap["error"]; ok {
		return fail.New(fmt.Sprintf("%v", errMsg))
	}

	return nil
}
func (client baseClientImp) ListAlias(ctx context.Context, aliasName string) (Indices, error) {
	indices := Indices{}
	request, err := http.NewRequest(http.MethodGet, client.rawIndexURL(aliasName), bytes.NewBufferString(""))
	if err != nil {
		return indices, fail.Wrap(err)
	}

	if client.User.Valid && client.Pass.Valid && client.User.String != "" && client.Pass.String != "" {
		request.SetBasicAuth(client.User.String, client.Pass.String)
	}

	response, err := client.HttpClient.Do(request)
	if err != nil {
		return indices, fail.Wrap(err)
	}
	responseMap := map[string]interface{}{}

	responseBody, err := ioutil.ReadAll(response.Body)
	err = json.Unmarshal(responseBody, &responseMap)
	if err != nil {
		return indices, fail.Wrap(err)
	}
	defer response.Body.Close()

	if errMsg, ok := responseMap["error"]; ok {
		return indices, fail.New(fmt.Sprintf("%v", errMsg))
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
func (client baseClientImp) ListTask(ctx context.Context) (Tasks, error) {
	return Tasks{}, nil
}
func (client baseClientImp) GetTask(ctx context.Context, taskID string) (Task, error) {
	request, err := http.NewRequest(http.MethodGet, client.taskURL(taskID), bytes.NewBufferString(""))
	if err != nil {
		return Task{}, fail.Wrap(err)
	}

	if client.User.Valid && client.Pass.Valid && client.User.String != "" && client.Pass.String != "" {
		request.SetBasicAuth(client.User.String, client.Pass.String)
	}

	response, err := client.HttpClient.Do(request)
	if err != nil {
		return Task{}, fail.Wrap(err)
	}
	defer response.Body.Close()

	responseMap := map[string]interface{}{}

	responseBody, err := ioutil.ReadAll(response.Body)
	err = json.Unmarshal(responseBody, &responseMap)
	if err != nil {
		return Task{}, fail.Wrap(err)
	}

	if errMsg, ok := responseMap["error"]; ok {
		return Task{}, fail.New(fmt.Sprintf("%v", errMsg))
	}

	if _, ok := responseMap["completed"].(bool); !ok {
		return Task{}, fail.New(fmt.Sprintf("Failed to extract completed from resposne"))
	}

	return Task{Complete: responseMap["completed"].(bool)}, nil
}

// Util
func (client baseClientImp) Version(ctx context.Context) (Version, error) {
	request, err := http.NewRequest(http.MethodGet, client.baseURL(), bytes.NewBufferString(""))
	if err != nil {
		return Version{}, fail.Wrap(err)
	}

	if client.User.Valid && client.Pass.Valid && client.User.String != "" && client.Pass.String != "" {
		request.SetBasicAuth(client.User.String, client.Pass.String)
	}

	response, err := client.HttpClient.Do(request)
	if err != nil {
		return Version{}, fail.Wrap(err)
	}
	defer response.Body.Close()

	responseMap := map[string]interface{}{}

	responseBody, err := ioutil.ReadAll(response.Body)
	err = json.Unmarshal(responseBody, &responseMap)
	if err != nil {
		return Version{}, fail.Wrap(err)
	}

	if errMsg, ok := responseMap["error"]; ok {
		return Version{}, fail.New(fmt.Sprintf("%v", errMsg))
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

	if client.User.Valid && client.Pass.Valid && client.User.String != "" && client.Pass.String != "" {
		request.SetBasicAuth(client.User.String, client.Pass.String)
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
	if client.Port == "None" {
		return client.Host
	}
	return client.Host + ":" + client.Port
}
func (client baseClientImp) listIndexURL() string {
	return client.baseURL() + "/_aliases"
}
func (client baseClientImp) indexURL(indexName string) string {
	return client.baseURL() + "/" + indexName + "/" + client.Type
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
	return client.baseURL() + "/" + indexOrAliasName + "/" + "_mapping" + "/" + client.Type
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
