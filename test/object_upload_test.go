package test

import (
	"fmt"
	"ossSdk"
	"testing"
)

//测试单个文件上传
func TestUploadObject(t *testing.T) {
	//请求报文
	req := oss.AccessVerify{
		AccessId:  "fairzhang1",
		AccessKey: "fairzhang1",
	}
	//return
	//连接服务器
	ip := "localhost:28082"
	//ip := "9.135.155.237:28082"
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

	////创建桶
	bucketOpt := oss.BucketCreateInput{
		BucketName: "example_bucket011",
	}
	err = client.Bucket.Create(bucketOpt)
	if err != nil {
		fmt.Printf("Create bucket:%v failed:%v\n", bucketOpt, err)
		return
	} else {
		fmt.Printf("Create bucket:%v success!\n", bucketOpt)
	}
	//
	testFilepath := "/home/fair/Study/Project/internship/nsosServer-go-sdk/test/picture/penguin.jpg"
	//testFilepath := "/home/fair/Study/Project/internship/nsosServer-go-sdk/test/picture/night.jpg"
	//testFilepath := "/home/fair/Study/Project/internship/nsosServer-go-sdk/test/picture/03.png"
	//testFilepath := "/home/fair/Study/Project/internship/nsosServer-go-sdk/test/picture/09.png"
	//testFilepath := "/home/fair/Study/Project/internship/nsosServer-go-sdk/test/picture/10.png"

	//testFilepath := "/home/fair/Study/Project/internship/nsosServer-go-sdk/test/04.png"
	//向桶中上传对象
	objectUploadOpt := oss.UploadInput{
		BucketName:   bucketOpt.BucketName,
		FilePath:     testFilepath,
		DataShards:   4, //EC码数据分片
		ParityShards: 2, //EC码冗余分片
	}
	err = client.Object.Upload(objectUploadOpt)
	if err != nil {
		fmt.Printf("Upload file:%v failed:%v\n", objectUploadOpt, err)
		return
	}
	fmt.Printf("Upload file:%v success\n", objectUploadOpt)

}
