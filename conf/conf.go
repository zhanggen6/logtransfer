package conf

//logtransfer配置
type LogtransferConfig struct {
	Kafkacfg `ini:"kafka"`
	EScfg    `ini:"es"`
}

//kafka配置
type Kafkacfg struct {
	Adress string `ini:"adress"`
	Topic string `ini:"topic"`
}

//elasticsearch配置
type EScfg struct {
	Adress string `ini:"adress"`
}