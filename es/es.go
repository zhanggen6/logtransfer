package es

import (
	"context"
	"github.com/olivere/elastic/v7"
	"fmt"
	"strings"
)

var(
	client *elastic.Client
)
//初始化elastic search，准备接受kafka发来的数据
func Init(addr string)(err error){
	fmt.Println("开始初始化elastic search.......")
	if !strings.HasPrefix(addr,"http://"){
		addr="http://"+addr
	}
	client,err=elastic.NewClient(elastic.SetURL(addr))
	if err != nil {
		return err
	}
	fmt.Println(client)
	return err
}

//SendToES 从kafka获取待数据发送到es
func SendToES(index,typ string, doc interface{} ) {
	//index(db)type(table)docment(row)
	fmt.Println("开始发送数据到elastic search")
	put1,err:=client.Index().Index(index).Type(typ).BodyJson(doc).Do(context.TODO())
	if err != nil {
		panic(err)
	}
	//创建index成功
	fmt.Println("elasticSearch创建数据成功",put1.Id,put1.Index,put1.Type)
}