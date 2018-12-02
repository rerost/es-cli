package es_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/rerost/es-cli/infra/es"
	"github.com/rerost/es-cli/setting"
)

func TestNewClient(t *testing.T) {
	t.Parallel()
	ctx := helperCreateValidContext()
	httpClient := new(http.Client)
	_, err := es.NewBaseClient(ctx, httpClient)
	if err != nil {
		t.Errorf("Failed to create base client: %v", err)
	}
}
func TestListIndex(t *testing.T) {
	t.Parallel()
	type InOutPairs struct {
		name   string
		out    map[string]interface{}
		esResp string
	}
	inOutPairs := []InOutPairs{
		{
			name: "when es return error",
			out: map[string]interface{}{
				"error":   "map[message:test]",
				"indices": es.Indices{},
			},
			esResp: `
{
	"error": {
		"message": "test"
	}
}`,
		},
		{
			name: "when es return aliases",
			out: map[string]interface{}{
				"error":   "",
				"indices": es.Indices{{Name: "test"}},
			},
			esResp: `
{
	"test": {
		"aliases": {}
	}
}`,
		},
	}

	for _, inOut := range inOutPairs {
		t.Run(inOut.name, func(t *testing.T) {
			t.Parallel()
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprintln(w, inOut.esResp)
			}))
			defer ts.Close()

			ctx := context.Background()
			host := ts.URL
			ctx = context.WithValue(ctx, setting.SettingKey("host"), host)
			ctx = context.WithValue(ctx, setting.SettingKey("type"), "_doc")
			baseClient, _ := es.NewBaseClient(ctx, ts.Client())
			indices, err := baseClient.ListIndex(ctx)

			if err != nil {
				if diff := cmp.Diff(inOut.out["error"], err.Error()); diff != "" {
					t.Errorf("Not mutch indices, diff(-want, +got) %s", diff)
				}
			}

			if diff := cmp.Diff(inOut.out["indices"].(es.Indices), indices); diff != "" {
				t.Errorf("Not mutch indices, diff(-want, +got) %s", diff)
			}
		})
	}
}

func helperCreateValidContext() context.Context {
	ctx := context.Background()
	ctx = context.WithValue(ctx, setting.SettingKey("host"), "http://localhost")
	ctx = context.WithValue(ctx, setting.SettingKey("type"), "_doc")
	return ctx
}
