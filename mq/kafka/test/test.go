package main

import (
	"encoding/json"
	"fmt"
	"gitee.com/guolianyu/pkg/mq/kafka"
	"github.com/Shopify/sarama"
	"time"
)

func main() {
	//同步生产者
	ProducerSyncMsg()
	//异步生产者
	//ProducerAsyncMsg()
	//消费者
	//ConsumerMsg()
}

type Msg struct {
	ID       int64  `json:"id"`
	Name     string `json:"name"`
	CreateAt int64  `json:"create_at"`
}

func ProducerAsyncMsg() {
	err := kafka.InitAsyncKafkaProducer(kafka.DefaultKafkaAsyncProducer, []string{"192.168.161.130:9092"}, nil)
	if err != nil {
		fmt.Println("InitAsyncKafkaProducer error", err)
	}
	msg := Msg{
		ID:       1,
		Name:     "test name async",
		CreateAt: time.Now().Unix(),
	}
	msgBody, _ := json.Marshal(msg)
	err = kafka.NewKafkaAsyncProducer(kafka.DefaultKafkaAsyncProducer).AsyncSend(&sarama.ProducerMessage{
		Topic: "lianyu-topic",
		Value: kafka.KafkaMsgValueEncoder(msgBody),
	})
	if err != nil {
		fmt.Println("send msg error ", err)
	} else {
		fmt.Println("send msg success")
	}
	//异步提交需要等待
	time.Sleep(3 * time.Second)
}

func ProducerSyncMsg() {
	err := kafka.InitSyncKafkaProducer(kafka.DefaultKafkaSyncProducer, []string{"192.168.161.130:9092"}, nil)
	if err != nil {
		fmt.Println("syncproducer err ", err)
	}
	msg := Msg{
		ID:       2,
		Name:     "testSync",
		CreateAt: time.Now().Unix(),
	}
	msgBody, _ := json.Marshal(msg)
	partion, offset, err := kafka.NewKafkaSyncProducer(kafka.DefaultKafkaSyncProducer).SyncSend(&sarama.ProducerMessage{
		Topic: "lianyu-topic",
		Value: kafka.KafkaMsgValueEncoder(msgBody),
	})
	if err != nil {
		fmt.Println("send msg error", err)
	} else {
		fmt.Println("send msg success partion", partion, "offset", offset)
	}

}

// 消费
func ConsumerMsg() {
	err := kafka.StartKafkaConsumer([]string{"192.168.161.130:9092"}, []string{"lianyu-topic"}, "test-group", nil, "sticky")
	if err != nil {
		fmt.Println(err)
	}
	//signals := make(chan os.Signal, 1)
	//signal.Notify(signals, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	//select {
	//case s := <-signals:
	//	kafka.KafkaStdLogger.Println("kafka test receive system signal:", s)
	//	return
	//}
}

//func msgHandler(message *sarama.ConsumerMessage) (bool, error) {
//	fmt.Println("消费消息:", "topic:", message.Topic, "Partition:",
//		message.Partition, "Offset:", message.Offset, "value:", string(message.Value))
//	msg := Msg{}
//	err := json.Unmarshal(message.Value, &msg)
//	if err != nil {
//		logger.Error("Unmarshal error", zap.Error(err))
//		return false, err
//	}
//	fmt.Println("msg : ", msg)
//	return true, nil
//}
