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
		cctx = context.WithValue(cctx, SettingKey("port"), port)
	}
	if docType != "" {
		cctx = context.WithValue(cctx, SettingKey("type"), docType)
	}
	if user != "" {
		cctx = context.WithValue(cctx, SettingKey("user"), user)
	}
	if pass != "" {
		cctx = context.WithValue(cctx, SettingKey("pass"), pass)
	}
	return cctx
}
