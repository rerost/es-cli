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

type Version struct{}

// Client is http wrapper
type BaseClient interface {
	// Index
	ListIndex(ctx context.Context) (Indices, error)
	CreateIndex(ctx context.Context, indexName string, mappingJSON string) error
	CopyIndex(ctx context.Context, srcIndexName string, dstIndexName string) (Task, error)
	DeleteIndex(ctx context.Context, indexName string) error
	CountIndex(ctx context.Context, indexName string) (Count, error)

	// Mapping
	GetMapping(ctx context.Context, indexOrAliasName string) (Mapping, error)
	// UpdateMapping(ctx context.Context, aliasName string, mappingJSON string) error

	// Alias
	CreateAlias(ctx context.Context, aliasName string, indexName string) error
	DropAlias(ctx context.Context, aliasName string, opts []Opt) error
	AddAlias(ctx context.Context, aliasName string, indexNames ...string) error
	RemoveAlias(ctx context.Context, aliasName string, indexNames ...string) error
	GetAlias(ctx context.Context, aliasName string) (Indices, error)

	// Task
	ListTask(ctx context.Context) (Tasks, error)
	GetTask(ctx context.Context, taskID string) (Task, error)

	Version(ctx context.Context) (Version, error)
	Ping(ctx context.Context) (bool, error)
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

	_host, ok := ctx.Value(setting.SettingKey("Host")).(string)
	if !ok {
		return client, fail.New("Failed to extract host")
	}

	_port, ok := ctx.Value(setting.SettingKey("Port")).(string)
	if !ok {
		return client, fail.New("Failed to extract port")
	}

	_type, ok := ctx.Value(setting.SettingKey("Type")).(string)
	if !ok {
		return client, fail.New("Failed to extract type")
	}

	client.Host = _host
	client.Port = _port
	client.Type = _type

	_user, ok := ctx.Value(setting.SettingKey("User")).(string)
	if ok {
		client.User = null.StringFrom(_user)
	}

	_pass, ok := ctx.Value(setting.SettingKey("Pass")).(string)
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

	if client.User.Valid && client.Pass.Valid {
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

	if client.User.Valid && client.Pass.Valid {
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

	if client.User.Valid && client.Pass.Valid {
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

	if client.User.Valid && client.Pass.Valid {
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

	if client.User.Valid && client.Pass.Valid {
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

// Mapping
func (client baseClientImp) GetMapping(ctx context.Context, indexOrAliasName string) (Mapping, error) {
	request, err := http.NewRequest(http.MethodGet, client.mappingURL(indexOrAliasName), bytes.NewBufferString(""))
	if err != nil {
		return Mapping(""), fail.Wrap(err)
	}

	if client.User.Valid && client.Pass.Valid {
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
func (client baseClientImp) CreateAlias(ctx context.Context, aliasName string, indexName string) error {
	createAliasJSON := fmt.Sprintf(`
{
	"actions": [
		{
			"add": {
				"index": "%s",
				"alias": "%s"
			}
		}
	]
}`, indexName, aliasName)

	request, err := http.NewRequest(http.MethodPost, client.aliasURL(), bytes.NewBufferString(createAliasJSON))
	request.Header.Add("Content-Type", "application/json")
	if err != nil {
		return fail.Wrap(err)
	}

	if client.User.Valid && client.Pass.Valid {
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

// TODO implement
func (client baseClientImp) DropAlias(ctx context.Context, aliasName string, opts []Opt) error {
	return nil
}
func (client baseClientImp) AddAlias(ctx context.Context, aliasName string, indexNames ...string) error {
	actions := []string{}
	for _, indexName := range indexNames {
		actions = append(actions, fmt.Sprintf(`
{
	"add": "%s",
	"alias": "%s"
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

	if client.User.Valid && client.Pass.Valid {
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
	"remove": "%s",
	"alias": "%s"
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

	if client.User.Valid && client.Pass.Valid {
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
func (client baseClientImp) GetAlias(ctx context.Context, aliasName string) (Indices, error) {
	return Indices{}, nil
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

	if client.User.Valid && client.Pass.Valid {
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

	if client.User.Valid && client.Pass.Valid {
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

	if _, ok := responseMap["version"].(Version); !ok {
		return Version{}, fail.New(fmt.Sprintf("Failed to extract completed from resposne"))
	}

	return responseMap["version"].(Version), nil
}
func (client baseClientImp) Ping(ctx context.Context) (bool, error) {
	request, err := http.NewRequest(http.MethodGet, client.baseURL(), bytes.NewBufferString(""))
	if err != nil {
		return false, fail.Wrap(err)
	}

	if client.User.Valid && client.Pass.Valid {
		request.SetBasicAuth(client.User.String, client.Pass.String)
	}

	response, err := client.HttpClient.Do(request)
	if err != nil {
		return false, fail.Wrap(err)
	}
	defer response.Body.Close()

	if response.StatusCode != 200 {
		responseBody, _ := ioutil.ReadAll(response.Body)
		return false, fail.New(string(responseBody))
	}

	return true, nil
}

func (client baseClientImp) baseURL() string {
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
	return client.baseURL() + "/" + indexOrAliasName
}
func (client baseClientImp) aliasURL() string {
	return client.baseURL() + "/_aliases"
}
func (client baseClientImp) countURL(indexName string) string {
	return client.indexURL(indexName) + "/_count"
}

func addParams(req *http.Request, params map[string]string) *http.Request {
	q := req.URL.Query()
	for k, v := range params {
		q.Add(k, v)
	}

	req.URL.RawQuery = q.Encode()
	return req
}
