package kafka

import (
	"fmt"
	"github.com/Shopify/sarama"
	"jd.com/logtransfer/es"
	"time"
)

//日志数据
type LogData struct {
	Time string
	Logcontent  string
}


//Init kafka连接
func Init(address []string,topic string)(error){
	saranaConfig:=sarama.NewConfig()
	consumer,err:=sarama.NewConsumer(address,saranaConfig)
	if err != nil {
		fmt.Println("kafka连接失败")
		return err
	}
	partionList,err:=consumer.Partitions(topic)
	fmt.Println("topic is",topic)
	if err != nil {
		fmt.Println("获取分区列表失败",err)
		return err
	}
	fmt.Println("分区列表：",partionList)
	for _,partion := range partionList {
		pc,err:=consumer.ConsumePartition(topic,int32(partion),sarama.OffsetNewest)
		if err != nil {
			fmt.Printf("消费分区%d失败，%v\n",partion,err)
			return err
		}
		//defer pc.AsyncClose()
		//自执行函数：参数：(partitionConsumer sarama.PartitionConsumer) 传参：(pc)
		go func(partitionConsumer sarama.PartitionConsumer){
			for msg:=range pc.Messages(){
				fmt.Printf("分区:%d offset:%d key:%v value:%v\n",msg.Partition,msg.Offset,msg.Key,string(msg.Value))
				now := time.Now().Format("2006:1:2 3-4-5")
				ld:=LogData{now,string(msg.Value)}
				es.SendToES(topic,"log",ld)
			}

		}(pc)


	}
	return err
}
