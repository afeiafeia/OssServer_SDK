package test

import (
	"fmt"
	"ossSdk"
	"testing"
)

//连接时的身份验证的测试
func TestNewClient(t *testing.T) {

	//请求报文
	req := oss.AccessVerify{
		AccessId:  "fairzhang",
		AccessKey: "fairzhang",
	}
	ip := "localhost:28082"
	client, err := oss.NewOssClient(ip, req)
	if err != nil {
		fmt.Printf("New client failed:%v", err)
		return
	}
	if client == nil {
		fmt.Printf("connect failed\n")
	} else {
		fmt.Printf("connect success!\n")
	}
	return

}
