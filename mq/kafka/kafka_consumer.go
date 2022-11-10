package kafka

import (
	"context"
	"github.com/Shopify/sarama"
	"log"
	"os"
	"os/signal"
	"syscall"

	"sync"
	"time"
)

/**
//定义消费者结构体
//获取消费者配置
//判断是否连接了kafka，断开重连
//启动消费者
//消费者消费数据
//程序关闭之后的退出
*/

// Consumer 定义消费者结构体
type Consumer struct {
	ready chan bool
}

// 获取消费者配置
func getKafkaDefaultConsumerConfig(assignor string) (config *sarama.Config) {
	config = sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	config.Consumer.MaxProcessingTime = 500 * time.Millisecond //消息入队时间
	config.Consumer.Fetch.Default = 1024 * 1024 * 2

	//多个消费集群
	config.Consumer.Group.Session.Timeout = 20 * time.Second   //消费者是否存活的心跳检测，默认是10秒，对应kafka session.timeout.ms配置
	config.Consumer.Group.Heartbeat.Interval = 6 * time.Second //消费者协调器心跳间隔时间，默认3s此值设置不超过group session超时时间的三分之一
	//消费组分区分配策略
	switch assignor {
	case "sticky":
		config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.BalanceStrategySticky}
	case "roundrobin":
		config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.BalanceStrategyRoundRobin}
	case "range":
		config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.BalanceStrategyRange}
	default:
		log.Panicf("Unrecognized consumer group partition assignor: %s", assignor)
	}
	//config.Consumer.Group.Rebalance.Timeout = 3600 //此配置是重新平衡时消费者加入group的超时时间，默认是60s
	config.Version = sarama.V2_1_1_0
	return
}

// StartKafkaConsumer 启动消费者（这些不要封装，消费代码写在程序中）
func StartKafkaConsumer(hosts, topics []string, groupID string, config *sarama.Config, assignor string) error {
	var err error
	if config == nil {
		config = getKafkaDefaultConsumerConfig(assignor)
	}
	consumer := &Consumer{
		ready: make(chan bool),
	}

	ctx, cancel := context.WithCancel(context.Background())
	client, err := sarama.NewConsumerGroup(hosts, groupID, config)
	if err != nil {
		return err
	}
	//consumptionIsPaused := false
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			//`Consume`应该在无限循环中调用，当服务器端重新平衡发生时，需要重新创建消费者会话以获得新的声明
			if err := client.Consume(ctx, topics, consumer); err != nil {
				log.Panicf("Error from consumer: %v", err)
			}
			// 检查上下文是否被取消，指示消费者应该停止
			if ctx.Err() != nil {
				return
			}
			consumer.ready = make(chan bool)
		}
	}()
	// 等待消费者设置完毕
	<-consumer.ready
	log.Println("Sarama consumer up and running!...")

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	keepRunning := true
	for keepRunning {
		select {
		case <-ctx.Done():
			log.Println("terminating: context cancelled")
			keepRunning = false
		case <-sigterm:
			log.Println("terminating: via signal")
			keepRunning = false
		}
	}
	cancel()
	wg.Wait()

	if err = client.Close(); err != nil {
		log.Panicf("Error closing client: %v", err)
	}
	return nil
}

// ConsumeClaim 开始消费数据,获取数据(这个程序内部已经go routing，在项目中只需要重写这个即可)
func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// 这个在go routing中调用
	// 参考 https://github.com/Shopify/sarama/blob/main/consumer_group.go#L27-L29
	for {
		select {
		case message := <-claim.Messages():
			//f(message)
			log.Printf("Message claimed: value = %s, timestamp = %v, topic = %s", string(message.Value), message.Timestamp, message.Topic)
			session.MarkMessage(message, "")

		// 当多实例加入时会陷入重平衡循环，解决方案如下
		// 参考 https://github.com/Shopify/sarama/issues/1192
		case <-session.Context().Done():
			return nil
		}
	}
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (c *Consumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(c.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (c *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}
