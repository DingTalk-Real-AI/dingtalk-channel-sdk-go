package safety

import (
	"context"
	"net"
	"net/url"
	"strings"

	"github.com/DingTalk-Real-AI/dingtalk-channel-sdk-go/types"
)

var blockedV4Blocks []*net.IPNet

func init() {
	cidrs := []string{
		"0.0.0.0/8", "10.0.0.0/8", "127.0.0.0/8", "169.254.0.0/16",
		"172.16.0.0/12", "192.168.0.0/16", "100.64.0.0/10",
		"192.0.0.0/24", "192.0.2.0/24", "198.18.0.0/15",
		"198.51.100.0/24", "203.0.113.0/24", "224.0.0.0/4", "240.0.0.0/4",
	}
	for _, c := range cidrs {
		_, block, err := net.ParseCIDR(c)
		if err == nil {
			blockedV4Blocks = append(blockedV4Blocks, block)
		}
	}
}

// hostAllowed 白名单匹配：精确主机名或 *.suffix 通配。
func hostAllowed(host string, allowlist []string) bool {
	if len(allowlist) == 0 || host == "" {
		return false
	}
	h := strings.ToLower(strings.TrimSuffix(host, "."))
	for _, e := range allowlist {
		e = strings.ToLower(strings.TrimSuffix(e, "."))
		if h == e {
			return true
		}
		if strings.HasPrefix(e, "*.") && strings.HasSuffix(h, e[1:]) {
			return true
		}
	}
	return false
}

// AssertPublicURL validates if a URL is safe to fetch 。
func AssertPublicURL(ctx context.Context, u string) error {
	return AssertPublicURLWithAllowlist(ctx, u, nil)
}

// AssertPublicURLWithAllowlist 带白名单的公网校验：命中主机名跳过校验（企业内网 CDN/专有云）。
func AssertPublicURLWithAllowlist(ctx context.Context, u string, allowlist []string) error {
	parsed, err := url.Parse(u)
	if err != nil {
		return &types.ChannelError{Code: types.ErrCodeSSRFBlocked, Message: "invalid url"}
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return &types.ChannelError{Code: types.ErrCodeSSRFBlocked, Message: "protocol " + parsed.Scheme}
	}
	host := parsed.Hostname()
	if hostAllowed(host, allowlist) {
		return nil
	}
	var ips []net.IP
	if ip := net.ParseIP(host); ip != nil {
		ips = append(ips, ip)
	} else {
		resolved, err := net.DefaultResolver.LookupIP(ctx, "ip", host)
		if err != nil {
			return &types.ChannelError{Code: types.ErrCodeSSRFBlocked, Message: "dns lookup failed: " + err.Error()}
		}
		ips = resolved
	}
	for _, ip := range ips {
		if ip.To4() != nil {
			if ipv4Blocked(ip.To4()) {
				return &types.ChannelError{Code: types.ErrCodeSSRFBlocked, Message: ip.String()}
			}
		} else if ipv6Blocked(ip) {
			return &types.ChannelError{Code: types.ErrCodeSSRFBlocked, Message: ip.String()}
		}
	}
	return nil
}

func ipv4Blocked(ip net.IP) bool {
	for _, block := range blockedV4Blocks {
		if block.Contains(ip) {
			return true
		}
	}
	return false
}

func ipv6Blocked(ip net.IP) bool {
	if ip.IsLoopback() || ip.IsUnspecified() || ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() || ip.IsPrivate() || ip.IsMulticast() {
		return true
	}
	if ip4 := ip.To4(); ip4 != nil {
		return ipv4Blocked(ip4)
	}
	lower := strings.ToLower(ip.String())
	return strings.HasPrefix(lower, "fe80:") ||
		strings.HasPrefix(lower, "fc") || strings.HasPrefix(lower, "fd") ||
		strings.HasPrefix(lower, "ff")
}
