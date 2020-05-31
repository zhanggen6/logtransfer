package main

import (
	"fmt"
	"gopkg.in/ini.v1"
	"jd.com/logtransfer/es"
	"jd.com/logtransfer/kafka"
	"jd.com/logtransfer/conf"
)

var(
	//返回指针类型的LogtransferConfig，以便于ini.MapTo函数修改
 appconf = new(conf.LogtransferConfig)
)

//将日志数据从kafka获取到，发往elasticsearch
func main() {
	//0.加载配置文件
	err:=ini.MapTo(appconf,"./conf/conf.ini")
	if err!=nil{
		fmt.Println("配置文件初始化失败",err)
		return
	}
	fmt.Println(appconf)
	//1.初始化elasticsearch
	err=es.Init(*appconf)
	if err != nil {
		fmt.Println("初始化ES失败",err)
		return
	}

	//2.初始化kafka:sara连接kafka的时候consumer.Partitions(topic)需要topic
	err=kafka.Init([]string{appconf.Kafkacfg.Adress},appconf.Kafkacfg.Topic)
	if err != nil {
		fmt.Println("初始化kafka失败",err)
		return
	}
	fmt.Println("kafka初始化成功！")
	select {
	}


}
