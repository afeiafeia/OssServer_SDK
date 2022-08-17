package test

import (
	"fmt"
	"ossSdk"
	"strconv"
	"sync"
	"testing"
)

//测试文件的批量获取
func TestBatchDownloadObject(t *testing.T) {

	var wg sync.WaitGroup
	for i := 1; i < 11; i++ {
		wg.Add(1)
		go func(j int) {
			DownFileSet(j)
			wg.Done()
		}(i)
	}

	wg.Wait()
	return

	//请求报文
	req := oss.AccessVerify{
		AccessId:  "fairzhang",
		AccessKey: "fairzhang",
	}
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
	//err = client.Bucket.Create(bucketOpt)
	//if err != nil {
	//	fmt.Printf("Create bucket:%v failed:%v\n", bucketOpt, err)
	//} else {
	//	fmt.Printf("Create bucket:%v success!\n", bucketOpt)
	//}

	folder := "/home/fair/Study/Project/internship/nsosServer-go-sdk/test/batchDownloadRes"
	//向桶中上传对象
	objectDownloadOpt := oss.DownloadInput{
		BucketName: bucketOpt.BucketName,
		Folder:     folder,
	}
	filePathSet := make([]string, 0)
	for i := 1; i < 10; i++ {
		fileName := "0" + strconv.Itoa(i) + ".png"
		filePathSet = append(filePathSet, fileName)

	}
	ten := "10.png"
	//night := "night.jpg"
	//penguin := "penguin.jpg"
	filePathSet = append(filePathSet, ten)
	//filePathSet = append(filePathSet, night)
	//filePathSet = append(filePathSet, penguin)
	for _, file := range filePathSet {
		objectDownloadOpt.FileName = file
		err = client.Object.Download(objectDownloadOpt)
		if err != nil {
			fmt.Printf("Download file:%v failed:%v\n", objectDownloadOpt, err)
			continue
		}
		fmt.Printf("Download file:%v success\n", objectDownloadOpt)
	}

	return

}

func DownFileSet(i int) {
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
	//err = client.Bucket.Create(bucketOpt)
	//if err != nil {
	//	fmt.Printf("Create bucket:%v failed:%v\n", bucketOpt, err)
	//} else {
	//	fmt.Printf("Create bucket:%v success!\n", bucketOpt)
	//}

	folder := "/home/fair/Study/Project/internship/nsosServer-go-sdk/test/batchDownloadRes" + strconv.Itoa(i)
	//向桶中上传对象
	objectDownloadOpt := oss.DownloadInput{
		BucketName: bucketOpt.BucketName,
		Folder:     folder,
	}
	filePathSet := make([]string, 0)
	for i := 1; i < 10; i++ {
		fileName := "0" + strconv.Itoa(i) + ".png"
		filePathSet = append(filePathSet, fileName)

	}
	ten := "10.png"
	//night := "night.jpg"
	//penguin := "penguin.jpg"
	//testTxt := "test.txt"
	filePathSet = append(filePathSet, ten)
	//filePathSet = append(filePathSet, night)
	//filePathSet = append(filePathSet, penguin)
	//filePathSet = append(filePathSet, testTxt)
	for _, file := range filePathSet {
		objectDownloadOpt.FileName = file
		err = client.Object.Download(objectDownloadOpt)
		if err != nil {
			fmt.Printf("Download file:%v failed:%v\n", objectDownloadOpt, err)
			continue
		}
		fmt.Printf("Download file:%v success\n", objectDownloadOpt)
	}

	return
}
