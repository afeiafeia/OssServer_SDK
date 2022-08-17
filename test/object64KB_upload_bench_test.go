package test

import (
	"fmt"
	"ossSdk"
	"ossSdk/log"
	"strconv"
	"sync"
	"testing"
)

//10w小文件上传测试
func TestBenchBatchUploadSmallObject(t *testing.T) {

	log.Init(log.ToLevel("ERROR"), "./log")
	var wg sync.WaitGroup
	for i := 1; i < 101; i++ {
		wg.Add(1)
		go func(userId int) {
			UploadFileToBatchBucket(userId)
			wg.Done()
		}(i)
	}

	wg.Wait()
	return
}

func UploadFileToBatchBucket(userId int) {
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
	for i := 1; i < 51; i++ {

		wgg.Add(1)
		go func(bucketId int, curClient *oss.OssClient) {
			UploadAllFiles(bucketId, userId, curClient)
			wgg.Done()
		}(i, client)
	}
	wgg.Wait()
	return
}

func UploadAllFiles(bucketId int, userId int, client *oss.OssClient) {
	bucketOpt := oss.BucketCreateInput{
		BucketName: "bucket" + strconv.Itoa(bucketId),
	}
	err := client.Bucket.Create(bucketOpt)
	if err != nil {
		//fmt.Printf("Create bucket:%v failed:%v\n", bucketOpt, err)
		//如果创建过bucket，继续上传即可
		log.Error("Create bucket:%v failed:%v\n", bucketOpt, err)
	} else {
		//fmt.Printf("Create bucket:%v success!\n", bucketOpt)
		log.Debug("Create bucket:%v success!\n", bucketOpt)
	}

	testFilepath := "/data/home/fairzhang/ossServer/TestFile/fairzhang" + strconv.Itoa(userId) + "/" + bucketOpt.BucketName

	//向桶中上传对象
	objectUploadOpt := oss.UploadInput{
		BucketName: bucketOpt.BucketName,
	}

	filePathSet := make([]string, 0)
	for j := 1; j < 51; j++ {
		filePath := testFilepath + "/text" + strconv.Itoa(j) + ".txt"
		filePathSet = append(filePathSet, filePath)
	}
	for _, file := range filePathSet {
		objectUploadOpt.FilePath = file
		err = client.Object.Upload(objectUploadOpt)
		if err != nil {
			//fmt.Printf("Upload file:%v failed:%v\n", objectUploadOpt, err)
			log.Error("Upload file:%v upload failed:%v\n", objectUploadOpt, err)
			continue
		}
		//fmt.Printf("Upload file:%v success\n", objectUploadOpt)
		log.Info("Upload file:%v upload success\n", objectUploadOpt)
	}
	return
}
