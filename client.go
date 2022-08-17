package oss

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"ossSdk/log"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/rangechow/errors"
)

const RECV_TIMEOUT = 30

const MESSAGE_BUFFER_MAX_SIZE = 1024 * 1024 // IPv6 TCP MSS 1220
const BUFFER_MAX_SIZE = 16 * 1024 * 1024

//回复报文中的状态:Ret
const (
	RET_ERROR = 400 //请求处理出错
	RET_OK    = 200 //请求处理成功
)

//从通道接受到的数据
type ChanContent struct {
	header Header
	body   []byte
}

//此结构参考COS设计
type OssClient struct {
	connector  net.Conn
	coder      SimpleCodec
	mu         sync.Mutex
	user       AccessVerify //认证信息
	common     Service
	Bucket     *BucketClient
	Object     *ObjectClient
	recvBuffer bytes.Buffer                  //接收缓冲区
	chans      map[uint32](chan ChanContent) //key是请求消息头部的MsgId，用来在MsgId对应请求的回复到来时通知客户端
	msgId      uint32                        //自增的msgId

}

type Service struct {
	client *OssClient
}
type BucketClient Service
type ObjectClient Service

//连接时的验证请求
type AccessVerify struct {
	AccessId  string `json:"accessId"`  //用户id
	AccessKey string `json:"accessKey"` //用户密钥
}

//连接验证的回复
type AccessVerifyRsp struct {
	Ret      int    `json:"ret"`      //回复状态码
	ErrorMsg string `json:"errorMsg"` //具体错误信息
}

func (client *OssClient) GetMsgId() uint32 {
	client.mu.Lock()
	client.msgId++
	msgId := client.msgId
	client.mu.Unlock()
	//msgId := (uint32)(1)
	return msgId
}
func NewOssClient(ipAddr string, verifyInfo AccessVerify) (*OssClient, error) {
	var client = &OssClient{
		user:  verifyInfo,
		msgId: 0,
	}
	client.chans = make(map[uint32]chan ChanContent)
	var err error
	client.connector, err = net.Dial("tcp", ipAddr)
	if err != nil {
		return nil, fmt.Errorf("connect to svr failed%v\n", err)
	}
	fmt.Printf("Verify user:%v\n", verifyInfo)

	cmd := "VerifyUser"
	msgId := client.GetMsgId()
	//log.Info("Send success!")
	// 创建接收通道
	rspChan := client.GenRspChan(msgId)
	defer client.DelRspChan(msgId)
	err = client.Send(cmd, msgId, verifyInfo)
	if err != nil {
		fmt.Printf("Send data failed: %v\n", err)
		return nil, fmt.Errorf("Send data failed: %v", err)
	}
	go client.RecviveResponse()
	timeOutChan := time.After(RECV_TIMEOUT * time.Second)
	rsp := &AccessVerifyRsp{}
	select {
	case <-timeOutChan:
		return nil, fmt.Errorf("time out")
	case response := <-rspChan:
		err = client.coder.Unmarshal(response.body, reflect.ValueOf(rsp))
		if err != nil {
			return nil, errors.New("UnMashal failed")
		}
	}

	if rsp.Ret == RET_ERROR {
		client.connector.Close()
		return nil, fmt.Errorf("Create client failed: %v", rsp.ErrorMsg)
	}
	client.common.client = client
	client.Bucket = (*BucketClient)(&client.common)
	client.Object = (*ObjectClient)(&client.common)
	log.Debug("connect to client success:%v\n", rsp)
	return client, nil
}

func (client *OssClient) GenRspChan(msgId uint32) chan ChanContent {
	rspChan := make(chan ChanContent, 10)
	client.mu.Lock()
	if _, ok := client.chans[msgId]; ok {
		fmt.Printf("Message:%v is exist", msgId)
	}
	client.chans[msgId] = rspChan
	client.mu.Unlock()
	return rspChan
}

func (client *OssClient) DelRspChan(msgId uint32) {
	client.mu.Lock()
	if _, ok := client.chans[msgId]; ok {
		close(client.chans[msgId])
		delete(client.chans, msgId)
	}
	client.mu.Unlock()
}

func (client *OssClient) Send(cmd string, msgId uint32, request interface{}) error {
	reqBytes := make([]byte, 0)
	if strings.Compare(cmd, "UploadSliceData") == 0 {
		sliceNew := request.(ObjectSliceUploadReqNew)
		reqBytes = sliceNew.Data

	} else {
		var errM error
		reqBytes, errM = client.coder.Marshal(reflect.ValueOf(request))
		if errM != nil {
			return fmt.Errorf("Marshal failed%v\n", errM)
		}
	}

	//构造头部
	reqMsgLen := (int32)(len(reqBytes))
	header := client.coder.SetHeader(msgId, cmd, reqMsgLen)
	if strings.Compare(cmd, "UploadSliceData") == 0 {
		sliceNew := request.(ObjectSliceUploadReqNew)
		taskId := sliceNew.TaskId
		header.SetTaskId(taskId)

	}
	headerBytes, errB := client.coder.GetHeaderBytes(header)
	if errB != nil {
		return fmt.Errorf("GetHeaderBytes failed:%v\n", errB)
	}

	rspMsgBytes := make([]byte, len(headerBytes))
	copy(rspMsgBytes, headerBytes)
	for _, b := range reqBytes {
		rspMsgBytes = append(rspMsgBytes, b)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	_, err := client.connector.Write(rspMsgBytes)
	if err != nil {
		fmt.Printf("Write rsp header failed %v\n", err)
		return fmt.Errorf("Write rsp header failed %v\n", err)
	}
	return nil
}

//response要传指针
func (client *OssClient) SendAndRecv(cmd string, request interface{}, response interface{}) error {

	msgId := client.GetMsgId()
	//log.Info("Send success!")
	// 创建接收通道
	timeOut := false
	rspChan := client.GenRspChan(msgId)
	defer func() {
		if timeOut {
			fmt.Printf("Del message:%v after time out\n", msgId)
		}
		client.DelRspChan(msgId)
	}()
	err := client.Send(cmd, msgId, request)
	if err != nil {
		fmt.Printf("Send data failed: %v", err)
		return fmt.Errorf("Send data failed: %v", err)
	}
	var rsp ChanContent
	timeOutChan := time.After(RECV_TIMEOUT * time.Second)
	//fmt.Printf("select\n")

	select {
	case <-timeOutChan:
		timeOut = true
		fmt.Printf("time out\n")
		return fmt.Errorf("time out")
	case rsp = <-rspChan:
		err = client.coder.Unmarshal(rsp.body, reflect.ValueOf(response))
		if err != nil {
			fmt.Printf("UnMashal failed")
			return errors.New("UnMashal failed")
		}
		return nil
	}

	return nil
}

func (client *OssClient) RecviveResponse() {
	msgBuf := make([]byte, MESSAGE_BUFFER_MAX_SIZE)
	hasHeader := false
	var header Header
	defer func() {
		fmt.Printf("Exist Receive!\n")
	}()
	for {
		client.connector.SetReadDeadline(time.Now().Add(time.Microsecond * 1))
		client.mu.Lock()
		recvSize, err := client.connector.Read(msgBuf)
		client.mu.Unlock()
		if err == io.EOF {
			//fmt.Printf("connection is closed by peer\n")
			log.Warn("connection is closed by peer\n")
			return
		}

		client.connector.SetReadDeadline(time.Time{}) // 清理掉deadLine，否则后续读取的时候之前设置的deadline还会生效
		if err != nil {
			if nerr, ok := err.(net.Error); !(ok && nerr.Timeout()) {
				fmt.Printf("Read err:%v\n", nerr)
				return
			}
		}
		if recvSize == 0 && client.recvBuffer.Len() == 0 {
			continue
		}
		//每次读取最多读取MESSAGE_BUFFER_MAX_SIZE数据，之后完全写入recvBuffer,recvBuffer的缓冲区会根据写入数据动态扩展
		//因此，如果recvSize!=writeSize,一定是写入过程出错了
		writeSize, err := client.recvBuffer.Write(msgBuf[:recvSize])
		if err != nil {
			fmt.Printf("write message to buffer failed %v", err)
			log.Error("write message to buffer failed %v", err)
			return
		}
		if writeSize != recvSize {
			fmt.Printf("buffer message failed, wirtesize %d, recvsize %d", writeSize, recvSize)
			log.Error("buffer message failed, wirtesize %d, recvsize %d", writeSize, recvSize)
			return
		}

		//获取头部
		if !hasHeader {
			//fmt.Printf("Judge header!\n")
			header, err = client.coder.GetHeader(&client.recvBuffer)
			if err != nil {
				if errors.Is(err, BUFFER_LT_HEADER) {
					//fmt.Printf("Read data not enough %d", recvSize)
					log.Warn("Read data not enough %d", recvSize)
					continue
				}
				fmt.Printf("Judge header failed:%v!\n", err)
				fmt.Printf("RecvBuffer len is %v and recvSize is:%v\n", client.recvBuffer.Len(), recvSize)
				return
			}
			//fmt.Printf("header: %s, buffer len: %d\n", client.header, client.recvBuffer.Len())
			log.Info("header: %s, buffer len: %d\n", header, client.recvBuffer.Len())
			//fmt.Printf("has header!\n")
			hasHeader = true
		}

		//如果得到了完整头部，尝试获取报文实体
		if hasHeader {
			//如果缓冲区剩余内容小于头部记录的报文实体长度，说明缓冲区中是不完整的报文实体，
			//需要继续读取，直至能得到完整报文实体，然后才进行逻辑处理
			if client.recvBuffer.Len() < header.GetPayloadLength() {
				continue
			}

			payload, err := client.coder.GetPayload(&client.recvBuffer, header.GetPayloadLength())
			if err != nil {
				//fmt.Printf("Get Payload failed %v", err)
				log.Error("Get Payload failed %v", err)
				hasHeader = false
				return
			}
			content := ChanContent{
				header: header,
				body:   payload,
			}
			//fmt.Printf("Get Body!\n")
			msgId := header.GetMsgId()
			client.mu.Lock()
			if _, ok := client.chans[msgId]; ok {
				client.chans[msgId] <- content
			} else {
				fmt.Printf("Message:%v is not exist\n", msgId)
			}
			//fmt.Printf("notify main coutine with msgId:%v by channel!\n", msgId)
			client.mu.Unlock()
			//fmt.Printf("notify main coutine!\n")
			hasHeader = false
		}

	}
}
