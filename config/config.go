package config

import "crypto/tls"

type Config struct {
	TLS   *tls.Config `json:"tls,omitempty"`
	Token string      `json:",omitempty"`
	URL   string      `json:"url,omitempty"`
}
