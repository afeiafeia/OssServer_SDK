package test

import (
	"fmt"
	"ossSdk"
	"testing"
)

//测试bucket的创建
func TestCreateBucket(t *testing.T) {
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

	bucketOpt := oss.BucketCreateInput{
		BucketName: "example_bucket",
	}
	err = client.Bucket.Create(bucketOpt)
	if err != nil {
		fmt.Printf("Create bucket:%v failed:%v\n", bucketOpt, err)
	} else {
		fmt.Printf("Create bucket:%v success!\n", bucketOpt)
	}

	return

}
