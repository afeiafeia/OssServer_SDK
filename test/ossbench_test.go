package test

import (
	"fmt"
	"ossSdk"
	"ossSdk/log"
	"strconv"
	"sync"
	"testing"
)

// 上报数据请求
type ECHOReq struct {
	Req int `json:"req"`
}

// 上报数据回包
type ECHORsp struct {
	Ret int `json:"ret"`
}

//echo压测
func TestOssEchoBench(t *testing.T) {

	log.Init(log.ToLevel("ERROR"), "./log")
	var wg sync.WaitGroup
	for i := 1; i < 101; i++ {
		wg.Add(1)
		go func(userId int) {
			BatchSendReq(userId)
			wg.Done()
		}(i)
	}

	wg.Wait()
	return
}

func BatchSendReq(userId int) {
	//请求报文
	req := oss.AccessVerify{
		AccessId:  "fairzhang",
		AccessKey: "fairzhang",
	}
	req.AccessId = "fairzhang" + strconv.Itoa(userId)
	req.AccessKey = "fairzhang" + strconv.Itoa(userId)
	//ip := "localhost:28082"
	//ip := "9.135.155.237:28082"
	ip := "9.134.120.127:28082"
	client, err := oss.NewOssClient(ip, req)
	if err != nil {
		//fmt.Printf("New client failed:%v", err)
		log.Error("New client failed:%v", err)
		return
	}
	if client == nil {
		fmt.Printf("connect failed\n")
		log.Error("connect failed\n")
		//continue
		return
	} else {
		//fmt.Printf("connect success!\n")
		log.Debug("connect success!\n")
	}
	var wgg sync.WaitGroup
	for i := 1; i < 301; i++ {

		wgg.Add(1)
		go func(curClient *oss.OssClient) {
			Echo(curClient)
			wgg.Done()
		}(client)
	}
	wgg.Wait()
	return
}

func Echo(client *oss.OssClient) {
	log.Debug("Echo...")
	request := &ECHOReq{
		Req: 200,
	}
	CMD := "ECHOFun"
	for i := 1; i < 401; i++ {

		rsp := &ECHORsp{}
		//log.Debug("Send...")
		client.SendAndRecv(CMD, request, rsp)
		//fmt.Printf("Response is :%v", rsp)

	}
	return
}
