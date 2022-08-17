package test

import (
	"fmt"
	"ossSdk"
	"strconv"
	"sync"
	"testing"
)

//测试文件批量上传
func TestBenchUploadObject(t *testing.T) {

	var wg sync.WaitGroup
	for i := 1; i < 11; i++ {
		wg.Add(1)
		go func(j int) {
			UploadFileSet(j)
			wg.Done()
		}(i)
	}

	wg.Wait()
	return
}

func UploadFileSet(i int) {
	//请求报文
	req := oss.AccessVerify{
		AccessId:  "fairzhang",
		AccessKey: "fairzhang",
	}
	req.AccessId = "fairzhang" + strconv.Itoa(i)
	req.AccessKey = "fairzhang" + strconv.Itoa(i)
	//连接服务器
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

	////创建桶
	bucketOpt := oss.BucketCreateInput{
		BucketName: "example_bucket006",
	}
	err = client.Bucket.Create(bucketOpt)
	if err != nil {
		fmt.Printf("Create bucket:%v failed:%v\n", bucketOpt, err)
	} else {
		fmt.Printf("Create bucket:%v success!\n", bucketOpt)
	}

	//testFilepath := "/home/fair/Study/Project/internship/nsosServer-go-sdk/test/image_test013.png"
	//testFilepath := "/home/fair/Study/Project/internship/nsosServer-go-sdk/test/penguin.jpg"
	//testFilepath := "/home/fair/Study/Project/internship/nsosServer-go-sdk/test/02.png"
	//testFilepath := "/home/fair/Study/Project/internship/nsosServer-go-sdk/test/09.png"
	//testFilepath := "/home/fair/Study/Project/internship/nsosServer-go-sdk/test/10.png"
	//向桶中上传对象
	objectUploadOpt := oss.UploadInput{
		BucketName: bucketOpt.BucketName,
	}

	filePathSet := make([]string, 0)
	testFilepath := "/home/fair/Study/Project/internship/nsosServer-go-sdk/test/"
	for i := 1; i < 10; i++ {
		filePath := testFilepath + "0" + strconv.Itoa(i) + ".png"
		filePathSet = append(filePathSet, filePath)
	}
	ten := "/home/fair/Study/Project/internship/nsosServer-go-sdk/test/10.png"
	//night := "/home/fair/Study/Project/internship/nsosServer-go-sdk/test/night.jpg"
	//penguin := "/home/fair/Study/Project/internship/nsosServer-go-sdk/test/penguin.jpg"
	//testTxt := "/home/fair/Study/Project/internship/nsosServer-go-sdk/test/test.txt"
	filePathSet = append(filePathSet, ten)
	//filePathSet = append(filePathSet, night)
	//filePathSet = append(filePathSet, penguin)
	//filePathSet = append(filePathSet, testTxt)
	for _, file := range filePathSet {
		objectUploadOpt.FilePath = file
		err = client.Object.Upload(objectUploadOpt)
		if err != nil {
			fmt.Printf("Upload file:%v failed:%v\n", objectUploadOpt, err)
			continue
		}
		fmt.Printf("Upload file:%v success\n", objectUploadOpt)
	}

	return
}
