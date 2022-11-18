package main

import (
	"encoding/json"
	"fmt"
	"gitee.com/guolianyu/pkg/logger/v2"
	"gitee.com/guolianyu/pkg/mq/kafka/v2"
	"github.com/Shopify/sarama"
	"go.uber.org/zap"
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
	err := kafka.StartKafkaConsumer([]string{"192.168.161.130:9092"}, []string{"lianyu-topic"},
		"test-group", nil, "sticky", msgHandler)
	if err != nil {
		fmt.Println(err)
	}

	//log.Printf("我在该调用的地方调用了：Message claimed: value = %s, timestamp = %v, topic = %s", string(message.Value), message.Timestamp, message.Topic)
	//signals := make(chan os.Signal, 1)
	//signal.Notify(signals, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	//select {
	//case s := <-signals:
	//	kafka.KafkaStdLogger.Println("kafka test receive system signal:", s)
	//	return
	//}
}

// 回调函数，把得到的数据先放到本地参数里面再进行操作，防止超时
func msgHandler(message *sarama.ConsumerMessage) (bool, error) {
	fmt.Println("我在该调用的地方调用了,消费消息:", "topic:", message.Topic, "Partition:",
		message.Partition, "Offset:", message.Offset, "value:", string(message.Value))
	msg := Msg{}
	err := json.Unmarshal(message.Value, &msg)
	if err != nil {
		logger.Error("Unmarshal error", zap.Error(err))
		return false, err
	}
	fmt.Println("msg : ", msg)
	return true, nil
}
