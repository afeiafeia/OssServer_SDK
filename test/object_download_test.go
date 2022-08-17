package test

import (
	"fmt"
	"ossSdk"
	"testing"
)

//测试单个文件下载
func TestDownloadObject(t *testing.T) {
	//请求报文
	req := oss.AccessVerify{
		AccessId:  "fairzhang1",
		AccessKey: "fairzhang1",
	}
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
		//BucketName: "bucket9",
	}
	//err = client.Bucket.Create(bucketOpt)
	//if err != nil {
	//	fmt.Printf("Create bucket:%v failed:%v\n", bucketOpt, err)
	//} else {
	//	fmt.Printf("Create bucket:%v success!\n", bucketOpt)
	//}

	folder := "/home/fair/Study/Project/internship/nsosServer-go-sdk/test/download"
	//objectName := "image_test013.png"
	//objectName := "night.jpg"
	objectName := "penguin.jpg"
	//objectName := "02.png"
	//objectName := "03.png"
	//objectName := "10.png"
	//objectName := "03.png"
	//objectName := "text100.txt"
	//向桶中上传对象
	objectDownloadOpt := oss.DownloadInput{
		BucketName: bucketOpt.BucketName,
		FileName:   objectName,
		Folder:     folder,
	}
	err = client.Object.Download(objectDownloadOpt)
	if err != nil {
		fmt.Printf("Download file:%v failed:%v\n", objectDownloadOpt, err)
		return
	}
	fmt.Printf("Download file:%v success\n", objectDownloadOpt)

}
