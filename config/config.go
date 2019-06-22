package config

import (
	"encoding/json"
	"fmt"

	"github.com/srvc/fail"
)

type Config struct {
	Host     string `json:"host"`
	Type     string `json:"type"`
	User     string `json:"user"`
	Pass     string `json:"pass"`
	Insecure bool   `json:"insecure"` // Use null.Bool for overwrite.
	Verbose  bool   `json:"verbose"`
	Debug    bool   `json:"debug"`
}

func DefaultConfig() Config {
	return Config{
		Host: "http://localhost:9200",
		Type: "_doc",
	}
}

func LoadConfig(jsonBody []byte) (Config, error) {
	cfg := Config{}
	err := json.Unmarshal(jsonBody, &cfg)
	if err != nil {
		return Config{}, fail.Wrap(err)
	}

	return cfg, nil
}

func DumpConfig(cfg Config) ([]byte, error) {
	return json.Marshal(cfg)
}

func LoadConfigWithNamespace(jsonBody []byte, namespace string) (Config, error) {
	cfg := Config{}

	cfgJSON := map[string]interface{}{}
	err := json.Unmarshal(jsonBody, &cfgJSON)
	if err != nil {
		return cfg, fail.Wrap(err)
	}
	if _, ok := cfgJSON[namespace]; !ok {
		return cfg, fail.New(fmt.Sprintf("Not found namespace: %v", namespace))
	}

	namespaceCfg, err := json.Marshal(cfgJSON[namespace])
	if err != nil {
		return cfg, fail.Wrap(err)
	}

	cfg, err = LoadConfig(namespaceCfg)
	if err != nil {
		return cfg, fail.Wrap(err)
	}
	return cfg, nil
}

func Overwrite(cfgOrg, cfgOverwrite Config) Config {
	cfgDst := cfgOrg
	if h := cfgOverwrite.Host; h != "" {
		cfgDst.Host = cfgOverwrite.Host
	}
	if t := cfgOverwrite.Type; t != "" {
		cfgDst.Type = cfgOverwrite.Type
	}
	if u := cfgOverwrite.User; u != "" {
		cfgDst.User = cfgOverwrite.User
	}
	if p := cfgOverwrite.Pass; p != "" {
		cfgDst.Pass = cfgOverwrite.Pass
	}
	if i := cfgOverwrite.Insecure; i {
		cfgDst.Insecure = i
	}

	return cfgDst
}
