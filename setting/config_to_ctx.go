package setting

import (
	"context"
)

func ContextWithOptions(ctx context.Context, host string, port string, docType string, user string, pass string) context.Context {
	// For copy context
	cctx := context.WithValue(ctx, SettingKey(""), nil)
	if host != "" {
		cctx = context.WithValue(cctx, SettingKey("host"), host)
	}
	if port != "" {
		cctx = context.WithValue(cctx, SettingKey("port"), host)
	}
	if docType != "" {
		cctx = context.WithValue(cctx, SettingKey("type"), host)
	}
	if user != "" {
		cctx = context.WithValue(cctx, SettingKey("user"), host)
	}
	if pass != "" {
		cctx = context.WithValue(cctx, SettingKey("pass"), host)
	}
	return cctx
}
