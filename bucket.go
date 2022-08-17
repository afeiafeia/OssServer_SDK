package oss

import (
	"fmt"
	"time"
)

//创建bucket时的输入
type BucketCreateInput struct {
	BucketName string //桶名称
	//后期可添加权限
}

//查询、删除bucket时的输入
type BucketInput struct {
	BucketName string //桶名称
}

//桶的创建、查询、删除的请求
type BucketReq struct {
	BucketName string `json:"bucketName"` //桶名称
	OwnerId    string `json:"ownerId"`    //所属租户的id(accessId)
	TimeStamp  uint64 `json:"timeStamp"`  //时间戳:上传时的时间戳(秒级别)
}

//桶的创建、删除的回复
type BucketRsp struct {
	Ret      int    `json:"ret"`      //回复状态码
	ErrorMsg string `json:"errorMsg"` //具体错误信息
}

type BucketQueryRsp struct {
	Ret         int    `json:"ret"`         //回复状态码
	ErrorMsg    string `json:"errorMsg"`    //具体错误信息
	ObjectCount uint64 `json:"objectCount"` //bucket下的对象数量
}

//bucket创建
func (b *BucketClient) Create(input BucketCreateInput) error {

	req := BucketReq{
		BucketName: input.BucketName,
		OwnerId:    b.client.user.AccessId,
		TimeStamp:  (uint64)(time.Now().Unix()),
	}
	cmd := "BucketCreate"
	rsp := BucketRsp{}
	err := b.client.SendAndRecv(cmd, req, &rsp)
	if err != nil {
		return fmt.Errorf("Receive response failed:%v", err)
	}
	if rsp.Ret != RET_OK {
		//服务端处理失败
		return fmt.Errorf("create bucket failed at server:%v!", rsp.ErrorMsg)
	}
	return nil
}

//bucket删除:将删除bucket下的所有对象
func (b *BucketClient) Delete(input DownloadInput) error {
	req := BucketReq{
		BucketName: input.BucketName,
		OwnerId:    b.client.user.AccessId,
		TimeStamp:  (uint64)(time.Now().Unix()),
	}
	cmd := "BucketDelete"
	rsp := BucketRsp{}
	err := b.client.SendAndRecv(cmd, req, &rsp)
	if err != nil {
		return fmt.Errorf("Receive response failed:%v", err)
	}
	if rsp.Ret != RET_OK {
		//服务端处理失败
		return fmt.Errorf("delete bucket failed at server!")
	}
	return nil
}

//bucket查询：查询该bucket下有多少对象

func (b *BucketClient) Query(input BucketInput) error {
	req := BucketReq{
		BucketName: input.BucketName,
		OwnerId:    b.client.user.AccessId,
	}
	cmd := "BucketQuery"
	rsp := BucketRsp{}
	err := b.client.SendAndRecv(cmd, req, &rsp)
	if err != nil {
		return fmt.Errorf("Receive response failed:%v", err)
	}
	if rsp.Ret != RET_OK {
		//服务端处理失败
		return fmt.Errorf("delete bucket failed at server:%v!", rsp.ErrorMsg)
	}
	return nil
}
