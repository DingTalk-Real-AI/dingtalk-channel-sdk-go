package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	channel "github.com/DingTalk-Real-AI/dingtalk-channel-sdk-go"
)

// 使用 DWS 的指定 Profile 发送示例卡片并完成原卡片，不需要机器人凭据。
func main() {
	profile, recipient := os.Getenv("DWS_PROFILE"), os.Getenv("DWS_OPEN_DINGTALK_ID")
	if profile == "" || recipient == "" {
		log.Fatal("请设置 DWS_PROFILE 和 DWS_OPEN_DINGTALK_ID")
	}
	executable := os.Getenv("DWS_BIN")
	if executable == "" {
		executable = "dws"
	}
	client, err := channel.NewDWSA2UIClient(channel.DWSA2UIConfig{Command: []string{executable}, Profile: profile})
	if err != nil {
		log.Fatal(err)
	}
	read := func(name string) []json.RawMessage {
		raw, err := os.ReadFile(name)
		if err != nil {
			log.Fatal(err)
		}
		var messages []json.RawMessage
		if err := json.Unmarshal(raw, &messages); err != nil {
			log.Fatal(err)
		}
		return messages
	}
	ctx := context.Background()
	result, err := client.SendCard(ctx, channel.A2UITarget{OpenDingTalkID: recipient}, read("example/a2ui-card.json"))
	if err != nil {
		log.Fatal(err)
	}
	if result.BizID == "" {
		log.Fatal(result.UpdateWarning)
	}
	if _, err := client.UpdateCard(ctx, result.BizID, read("example/a2ui-update.json"), "FINISH"); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("示例卡片已完成，bizId=%s\n", result.BizID)
}
