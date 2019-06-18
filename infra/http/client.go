package http

import (
	"crypto/tls"
	nhttp "net/http"

	"github.com/rerost/es-cli/config"
)

func NewClient(cfg config.Config) *nhttp.Client {
	var httpClient *nhttp.Client
	if cfg.Insecure.Valid && cfg.Insecure.Bool {
		tr := &nhttp.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}
		httpClient = &nhttp.Client{Transport: tr}
	} else {
		httpClient = new(nhttp.Client)
	}

	return httpClient
}
