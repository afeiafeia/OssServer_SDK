//go:build linux
// +build linux

package oss

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"os"
	"ossSdk/log"
	"path"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/klauspost/reedsolomon"
	"github.com/rangechow/errors"
)

type UploadInput struct {
	BucketName string //桶名称
	FilePath   string //文件路径

	DataShards   int //数据分片的数量，如果没有给定值，默认值将被设为4
	ParityShards int //EC码分片的数量，如果没有给定值，默认值将被设为2
}

type DownloadInput struct {
	BucketName string //桶名称
	FileName   string //文件路径
	Folder     string //下载到的文件夹路径
}

//对象元数据
type ObjMetadata struct {
	Name              string      `json:"name"`              //对象名称(文件名称)
	ContentLength     uint64      `json:"contentLength"`     //文件大小，单位是byte
	ContentType       string      `json:"contentType"`       //文件类型
	ContentEncode     string      `json:"contentEncode"`     //编码方式
	Suffix            string      `json:"suffix"`            //后缀
	UploadTime        uint64      `json:"uploadTime"`        //文件的上传时间的时间戳(秒级别)
	Md5               string      `json:"md5"`               //上传时服务端计算的Md5的值
	SliceInfo         []SliceMeta `json:"sliceInfo"`         //分片信息，可确定分片的数量以及每一分片的大小
	OriDataSliceCount uint32      `json:"oriDataSliceCount"` //存储原始数据的分片数量
	ECCodeSliceCount  uint32      `json:"eccodeSliceCount"`  //存储EC码数据的分片数量
	IsEncript         uint32      `json:"isEncript"`         //是否进行了加密,0表示没有，1表示加密了
	EncriptAlgo       string      `json:"encriptAlgo"`       //加密算法
	Expire            uint64      `json:"expire"`            //将在expire时间后(Duration单位)过期
}
type SliceMeta struct {
	Num    uint32    `json:"num"`
	Length uint64    `json:"length"`
	Type   SliceType `json:"type"`
	Md5    string    `json:"md5"`
}

//对象元数据和数据分片分别使用一个数据结构，不复用
//对象元数据的上传请求
type ObjectMetadataUploadReq struct {
	UserId     string      `json:"userId"`     //用户id
	BucketName string      `json:"bucketName"` //桶名称
	ObjName    string      `json:"objName"`    //对象名称
	MetaInfo   ObjMetadata `json:"metaInfo"`   //对象的元数据信息
}

//对象元数据上传请求的回复
type ObjectMetadataUploadRsp struct {
	Ret      int    `json:"ret"`      //回复状态码
	ErrorMsg string `json:"errorMsg"` //具体错误信息
	TaskId   string `json:"taskId"`   //对象名称
}

//数据分片上传请求
type ObjectSliceUploadReq struct {
	UserId     string              `json:"userId"`     //用户id
	BucketName string              `json:"bucketName"` //桶名称
	ObjName    string              `json:"objName"`    //对象名称
	Slice      map[uint32]ObjSlice `json:"slice"`      //分片的数据,key是分片的编号
}

//数据分片上传请求
type ObjectSliceUploadReqNew struct {
	TaskId string
	Data   []byte
}

//数据分片上传的回复
type ObjectSliceUploadRsp struct {
	Ret      int    `json:"ret"`      //回复状态码
	ErrorMsg string `json:"errorMsg"` //具体错误信息
	ObjectId string `json:"objectId"`
}

//数据分片的类型：实际数据或者EC码
type SliceType uint8

const (
	SliceData   SliceType = iota //对象实际数据
	SliceECCode                  //对象的EC码
)

type ObjSlice struct {
	Type SliceType `json:"type"` //对象类型，可以是实际存储对象或者EC码
	Num  uint32    `json:"num"`  //编号(用于按序恢复原对象)
	Data []byte    `json:"data"` //分片数据
}

type ObjSliceGetRsp struct {
	Ret      int       `json:"ret"`      //回复状态码
	ErrorMsg string    `json:"errorMsg"` //具体错误信息
	ObjectId string    `json:"objectId"` //对象id
	Num      uint32    `json:"num"`      //编号(用于按序恢复原对象)
	NodeId   uint64    `json:"nodeId"`   //所在节点
	Type     SliceType `json:"type"`     //对象类型，可以是实际存储对象或者EC码
}

type ObjSliceDataQueryRsp struct {
	Ret      int       `json:"ret"`      //回复状态码
	ErrorMsg string    `json:"errorMsg"` //具体错误信息
	ObjectId string    `json:"objectId"` //对象id
	Num      uint32    `json:"num"`      //编号(用于按序恢复原对象)
	NodeId   uint64    `json:"nodeId"`   //所在节点
	Type     SliceType `json:"type"`     //对象类型，可以是实际存储对象或者EC码
	Data     []byte    `json:"data"`     //分片数据
}

//对象的查询、删除请求
type ObjectReq struct {
	UserId     string `json:"userId"`     //用户id
	BucketName string `json:"bucketName"` //桶名称
	ObjName    string `json:"objName"`    //对象名称
}

type QueryDataType uint8

const (
	ObjectMetaData  QueryDataType = iota //对象的元数据
	ObjectSliceData                      //对象的分片数据
)

type ObjectQueryRsp struct {
	Ret      int           `json:"ret"`      //回复状态码
	ErrorMsg string        `json:"errorMsg"` //具体错误信息
	DataType QueryDataType `json:"dataType"` //数据的类型
	Meta     ObjMetadata   `json:"meta"`     //原数据信息
	Slice    ObjSlice      `json:"slice"`    //分片数据
}

type ObjectData struct {
	Folder string      //文件下载到的目录，如果为空，下载到当前目录
	Meta   ObjMetadata //原数据信息
	Slice  []ObjSlice  //分片数据
}
type DownloadRes uint8

const (
	DOWNLOAD_OK       DownloadRes = iota //对象的元数据
	DOWNLOAD_CONTINUE                    //对象的分片数据
	DOWNLOAD_FAIL                        //对象的分片数据
)

//文件上传操作(暂时取消了Md5)
func (o *ObjectClient) Upload(input UploadInput) error {
	if input.DataShards <= 0 {
		input.DataShards = 4
	}
	if input.ParityShards <= 0 {
		input.ParityShards = 2
	}
	//meta, sliceInfo, data, err := EncodeDataToSlice(input.FilePath, input.DataShards, input.ParityShards)
	meta, _, data, err := EncodeDataToSlice(input.FilePath, input.DataShards, input.ParityShards)

	req := ObjectMetadataUploadReq{
		UserId:     o.client.user.AccessId,
		BucketName: input.BucketName,
		ObjName:    meta.Name,
		MetaInfo:   meta,
	}

	//fmt.Printf("Object's metadata is:%v\n", req)
	cmd := "UploadMetadata"
	rsp := ObjectMetadataUploadRsp{}
	err = o.client.SendAndRecv(cmd, req, &rsp)
	if err != nil {
		//fmt.Printf("Upload Metadata failed:%v when handle:%v\n", err, input)
		log.Error("Upload Metadata failed:%v when handle:%v\n", err, input)
		return err
	}
	if rsp.Ret != RET_OK {
		//服务端处理失败
		//fmt.Printf("upload metadata failed at server:%v when handle:%v!", rsp.ErrorMsg, input)
		log.Error("upload metadata failed at server:%v when handle:%v!", rsp.ErrorMsg, input)
		return fmt.Errorf("upload metadata failed at server:%v when handle:%v!", rsp.ErrorMsg, input)
	}
	//fmt.Printf("Metadata upload success!\n")
	//如果成功，从回复中拿到taskId,再构造分片数据上传请求，进行上传,它是服务端针对待上传对象生成的一个唯一性id

	//sliceReq := ObjectSliceUploadReq{
	//	UserId:     req.UserId,
	//	BucketName: req.BucketName,
	//	ObjName:    req.ObjName,
	//}
	//sliceReq.Slice = sliceInfo
	taskId := rsp.TaskId

	sliceData := ObjectSliceUploadReqNew{
		TaskId: taskId,
		Data:   data,
	}
	log.Debug("start upload sliceData:%v!\n", input)
	//发送分片数据的请求
	cmd = "UploadSliceData"
	sliceRsp := ObjectSliceUploadRsp{}
	//err = o.client.SendAndRecv(cmd, sliceReq, &sliceRsp)
	err = o.client.SendAndRecv(cmd, sliceData, &sliceRsp)
	if err != nil {
		//fmt.Printf("Upload slice data failed:%v when handle:%v!\n", err, input)
		log.Error("Upload slice data failed:%v when handle:%v!\n", err, input)
		return err
	}
	if sliceRsp.Ret != RET_OK {
		//服务端处理失败
		//fmt.Printf("Upload slice failed at server:%v when handle:%v\n", sliceRsp.ErrorMsg, input)
		log.Error("Upload slice failed at server:%v when handle:%v\n", sliceRsp.ErrorMsg, input)
		return fmt.Errorf("Upload slice failed at server:%v when handle:%v", sliceRsp.ErrorMsg, input)
	}
	log.Info("Upload sliceData success at object:%v!\n", input)
	//fmt.Printf("Upload sliceData success at object:%v!\n", input)
	return nil
}

func constructMetadata(filePath string) ObjMetadata {

	meta := ObjMetadata{}

	meta.Name = filepath.Base(filePath)
	meta.Suffix = filepath.Ext(filePath)

	return meta

}

//文件下载
//文件名可以不带后缀:指定文件下载到哪个文件夹下
func (o *ObjectClient) Download(input DownloadInput) error {
	folder := input.Folder
	bucketName := input.BucketName
	fileName := input.FileName

	objName := fileName
	queryReq := ObjectReq{
		UserId:     o.client.user.AccessId,
		BucketName: bucketName,
		ObjName:    objName,
	}

	msgId := o.client.GetMsgId()
	// 创建接收通道
	rspChan := o.client.GenRspChan(msgId)
	defer o.client.DelRspChan(msgId)
	cmd := "ObjectQuery"
	err := o.client.Send(cmd, msgId, queryReq)
	if err != nil {
		return fmt.Errorf("send request failed: %v", err)
	}

	// log.Info("send chan len %v", len(c.chans))

	// wait for channel
	var rsp ChanContent
	//此处的超时时间应该根据对象的大小调整？
	timeOutChan := time.After(RECV_TIMEOUT * time.Second)
	//等待超时
	response := &ObjectQueryRsp{}
	if strings.Compare(folder, "") == 0 {
		//如果为空，将folder设置为当前目录
		folder = getCurrentAbPathByCaller()
	}
	result := &ObjectData{
		Folder: folder,
	}
	result.Slice = make([]ObjSlice, 0)
	hasMeta := false
	for {
		select {
		case <-timeOutChan:
			return fmt.Errorf("time out")
		case rsp = <-rspChan:
			err = o.client.coder.Unmarshal(rsp.body, reflect.ValueOf(response))
			if err != nil {
				return errors.New("UnMashal failed")
			}
			//如果读取成功，针对解析情况进行处理：判断是否可以组装，如果可以，组装后得到文件，返回，否则继续读取，直到可以组装或者超时
			dataType := response.DataType
			if dataType == ObjectMetaData {
				//fmt.Printf("receive metadata\n")
				result.Meta = response.Meta
				hasMeta = true
			} else {
				//fmt.Printf("receive slicedata\n")
				result.Slice = append(result.Slice, response.Slice)
			}
			if hasMeta {
				//fmt.Printf("receive all slice data\n")
				state := HandleQueryRsp(result)
				if state == DOWNLOAD_FAIL {
					return fmt.Errorf("Download object failed:%v", err)
				} else if state == DOWNLOAD_OK {
					return nil
				} else {
					//fmt.Printf("Continue...")
				}
			}
		}
	}

	return nil
}

func HandleQueryRsp(res *ObjectData) DownloadRes {
	//对象分片中，数据分片的数量
	//EC码工作条件：原始对象被分成m份数据分片和n份EC码分片，当收到大于等于m份任意分片时，可以利用EC码恢复出原始数据
	sliceSumCount := (int)(res.Meta.OriDataSliceCount)
	curSliceCount := len(res.Slice)
	//当目前收到的分片数量小于m份时，继续等待后续分片的到来
	if curSliceCount < sliceSumCount {
		return DOWNLOAD_CONTINUE
	}
	//组装，并根据元数据创建指定类型文件，将数据写入
	//(1)组装：对于ObjectData的Slice数组，根据Num按照有小到大排序
	sliceSet := res.Slice
	sliceMap := make(map[int]ObjSlice)
	sort.Slice(sliceSet, func(i, j int) bool {
		return sliceSet[i].Num < sliceSet[j].Num
	})
	data := make([]byte, 0)
	for i, slice := range sliceSet {
		num := (int)(slice.Num)
		sliceMap[num] = sliceSet[i]
		for _, b := range slice.Data {
			data = append(data, b)
		}
	}
	dataShards := (int)(res.Meta.OriDataSliceCount)
	parityShards := (int)(res.Meta.ECCodeSliceCount)
	enc, err := reedsolomon.New(dataShards, parityShards)
	if err != nil {
		fmt.Printf("Create ECCode instance failed:%v", err)
		return DOWNLOAD_FAIL
	}
	shards := make([][]byte, dataShards+parityShards)
	for i := range shards {
		//分片的编号比在shrads中的下标大1
		num := i + 1
		if _, ok := sliceMap[num]; !ok {
			shards[i] = nil
		} else {
			shards[i] = sliceMap[num].Data
		}
	}
	ok, err := enc.Verify(shards)
	if !ok {
		//分片有缺失
		fmt.Printf("shard is less\n")
		err = enc.Reconstruct(shards)
		if err != nil {
			fmt.Printf("Reconstruct failed:%v", err)
			return DOWNLOAD_FAIL
		}
		ok, err = enc.Verify(shards)
		if err != nil {
			fmt.Printf("Verify failed after reconstruct:%v", err)
			return DOWNLOAD_FAIL
		}
	}

	//验证完成，可以进行原对象的恢复
	//路径可能不存在，先创建路径
	err = os.MkdirAll(res.Folder, os.ModePerm)
	if err != nil {
		fmt.Printf("creating folder: %v failed: %v", res.Folder, err)
		return DOWNLOAD_FAIL
	}

	//filePath := res.Folder + "/" + res.Meta.Name + "." + res.Meta.Suffix
	filePath := res.Folder + "/" + res.Meta.Name
	f, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatal("error opening file: %v", err)
	}

	err = enc.Join(f, shards, len(shards[0])*dataShards)
	if err != nil {
		fmt.Printf("Construct origin file failed:%v", err)
		return DOWNLOAD_FAIL
	}
	fmt.Printf("Decode success!\n")
	return DOWNLOAD_OK

	//组装完成之后校验md5
	originMd5Value := res.Meta.Md5

	newMd5Inst := md5.New()
	newMd5Inst.Write(data)
	newMd5Value := hex.EncodeToString(newMd5Inst.Sum(nil))

	if strings.Compare(originMd5Value, newMd5Value) != 0 {
		fmt.Printf("Md5 is not equal!\n")
		return DOWNLOAD_FAIL
	}

	writeSize, errW := f.Write(data)
	if errW != nil || writeSize != len(data) {
		fmt.Errorf("write data to file failed: %v", errW)
		return DOWNLOAD_FAIL
	}
	return DOWNLOAD_OK

}

func getCurrentAbPathByCaller() string {
	var abPath string
	_, filename, _, ok := runtime.Caller(0)
	if ok {
		abPath = path.Dir(filename)
	}
	return abPath
}

//对原始文件数据进行EC码编码
func EncodeDataToSlice(filePath string, dataShards, parityShards int) (ObjMetadata, map[uint32]ObjSlice, []byte, error) {
	//分片太多会很影响编码解码的效率，此处做个限制
	if (dataShards + parityShards) > 256 {
		fmt.Fprintf(os.Stderr, "Error: sum of data and parity shards cannot exceed 256\n")
		os.Exit(1)
	}

	//此处构造的元数据，只是记录了文件名、长度一些基本信息，而有关分片的详细信息，在后面赋值
	meta := constructMetadata(filePath)
	//sliceData用来记录分片数据，key是分片的编号
	sliceData := make(map[uint32]ObjSlice)
	//读取文件取出数据
	dataBytes, err := ioutil.ReadFile(filePath)
	if err != nil {
		fmt.Printf("error opening file: %v\n", err)
		return meta, nil, nil, fmt.Errorf("open file failed")
	}

	//计算文件长度：初始化元数据中的对象长度项
	length := len(dataBytes)
	meta.ContentLength = (uint64)(length)
	//初始化元数据中记录分片的数据分片数量和EC码分片的数量
	meta.OriDataSliceCount = (uint32)(dataShards)
	meta.ECCodeSliceCount = (uint32)(parityShards)

	//创建EC码编码实例
	enc, err := reedsolomon.New(dataShards, parityShards)
	if err != nil {
		log.Error("Create ECEncoder failed:%v", err)
		return meta, nil, nil, fmt.Errorf("Create ECEncoder failed:%v", err)
	}
	//将数据拆分
	shards, err := enc.Split(dataBytes)
	if err != nil {
		log.Error("Split data failed:%v", err)
		return meta, nil, nil, fmt.Errorf("Split data failed:%v", err)
	}
	//对数据编码，编码后，所有shard的长度是相等的
	err = enc.Encode(shards)
	if err != nil {
		log.Error("Encode data failed:%v", err)
		return meta, nil, nil, fmt.Errorf("Encode data failed:%v", err)
	}
	//计算分片详细信息，记录到元数据和分片中
	//Md5编码实例
	data := make([]byte, 0)
	for i, shard := range shards {
		sliceNum := (uint32)(i + 1)
		sliceMeta := SliceMeta{
			Num:    sliceNum,
			Length: (uint64)(len(shard)),
		}
		if i < dataShards {
			//分片为数据类型
			sliceMeta.Type = SliceData
		} else {
			//分片为EC码类型
			sliceMeta.Type = SliceECCode
		}
		md5Inst := md5.New()
		md5Inst.Write(shard)
		sliceMeta.Md5 = hex.EncodeToString(md5Inst.Sum(nil))
		meta.SliceInfo = append(meta.SliceInfo, sliceMeta)
		sliceData[sliceNum] = ObjSlice{
			Type: sliceMeta.Type,
			Num:  sliceNum,
			Data: shard,
		}
		for _, b := range shard {
			data = append(data, b)
		}
	}

	return meta, sliceData, data, nil
}
