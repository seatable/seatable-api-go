package context

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/mattn/go-isatty"
	"os"
)

type Context struct {
	ContextData map[string]interface{}
}

func New() (*Context, error) {
	var isCloud bool
	if getEnv("is_cloud") == "1" {
		isCloud = true
	}

	c := new(Context)
	c.ContextData = make(map[string]interface{})

	if isCloud && !isatty.IsTerminal(os.Stdin.Fd()) {
		var buf bytes.Buffer
		buf.ReadFrom(os.Stdin)
		err := json.Marshal(buf.Bytes(), &c.ContextData)
		if err != nil {
			err := fmt.Errorf("failed to parse json data: %v", err)
			return nil, err
		}
	}

	return c, nil
}

func getEnv(key string) string {
	return os.Getenv(key)
}

func (c *Context) ServerURL() string {
	return getEnv("dtable_web_url")
}

func (c *Context) APIToken() string {
	return getEnv("api_token")
}

func (c *Context) CurrentRow() interface{} {
	if c.ContextData == nil {
		return nil
	}
	return c.ContextData["row"]
}

func (c *Context) CurrentTable() interface{} {
	if c.ContextData == nil {
		return nil
	}
	return c.ContextData["table"]
}
