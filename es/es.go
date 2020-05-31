package es

import (
	"context"
	"github.com/olivere/elastic/v7"
	"fmt"
	"time"
	"jd.com/logtransfer/conf"
	"strings"
)

//日志数据
type LogData struct {
	Time       string
	Logcontent string
}

var (
	client *elastic.Client
	ESchan chan *LogData
)

//初始化elastic search，准备接受kafka发来的数据
func Init(appConf conf.LogtransferConfig) (err error) {
	fmt.Println("开始初始化elastic search.......")
	//初始化kafka消费到ES之间的通道，kafka-----chanel------>Es
	ESchan = make(chan *LogData,appConf.EScfg.ChanSize)
	if !strings.HasPrefix(appConf.EScfg.Adress, "http://") {
		appConf.EScfg.Adress = "http://" + appConf.EScfg.Adress
	}
	client, err = elastic.NewClient(elastic.SetURL(appConf.EScfg.Adress))
	if err != nil {
		return err
	}
	fmt.Println("初始化elastic search.......成功！")
	//开启20个gorutines从channel中获取kafka消息发送到ES
	for i := 0; i < 20; i++ {
		//从channel中获取消息发送到es
		go SendToES(i,appConf)
	}
	return err
}

func SendToChan(data *LogData) {
	ESchan <- data
	fmt.Printf("kafka消息发送到channel成功%s", data.Logcontent)
}

//SendToES 从kafka获取待数据发送到es
func SendToES(id int,appConf conf.LogtransferConfig) {
	fmt.Printf("开启gorutine-%d\n",id)
	for {
		select {
		case logs := <- ESchan:
			//index(db)type(table)docment(row)
			fmt.Printf("开始发送数据%s到elastic search", logs.Logcontent)
			put1, err := client.Index().Index(appConf.EScfg.Index).Type(appConf.Kafkacfg.Topic).BodyJson(logs).Do(context.TODO())
			if err != nil {
				fmt.Println(err)
				panic(err)
			}
			//创建index成功
			fmt.Println("elasticSearch创建数据成功", put1.Id, put1.Index, put1.Type)
		default:
			time.Sleep(time.Second)
		}
	}

}
