package kafka

import (
	"gitee.com/guolianyu/pkg/logger"
	"github.com/Shopify/sarama"
	"github.com/eapache/go-resiliency/breaker"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

/**
//逻辑图

//定义生产者结构体

//定义同步生产者结构体

//定义异步生产者结构体

//获取生产者的默认配置

//初始化“同步生产者”,并保持连接状态keepconnect是开启的、以及检查程序运行后连接状态是关闭的则将状态改为closed
//创建“同步生产者”的实例，外界可以调用，这样就可以调用实例的方法如send，sendmessage
//发送“同步生产者”Send消息
//发送“同步生产者”多条SendMessages
//“同步生产者”保持连接状态是开启的
//“同步生产者”检查在程序运行后连接状态是关闭则改为close

//初始化“异步生产者”,并保持连接状态keepconnect是开启的、以及检查连接状态是关闭的则将状态改为closed
//创建“异步生产者”的实例，外界可以调用，这样就可以调用实例的方法如send
//发送“异步生产者”send消息
//“异步生产者”保持连接状态是开启的
//“异步生产者”检查在程序运行后连接状态是关闭则改为close

//关闭“同步生产者”
//关闭“异步生产者”

*/

// KafkaProducer 定义生产者结构体
type KafkaProducer struct {
	Name       string           //生产者名称
	Hosts      []string         //支持kafka集群IP
	Config     *sarama.Config   //配置
	Status     string           //状态
	Breaker    *breaker.Breaker //断路器
	ReConnect  chan bool        // 重连通道bool类型
	StatusLock sync.Mutex       //锁
}

// SyncProducer 定义同步生产者结构体
type SyncProducer struct {
	KafkaProducer
	SyncProducer *sarama.SyncProducer //同步生产者
}

// AsyncProducer 定义异步生产者结构体
type AsyncProducer struct {
	KafkaProducer
	AsyncProducer *sarama.AsyncProducer //异步生产者
}

// 额外定义常量
const (
	kafkaProducerConnected    string = "Connected"    //连接
	kafkaProducerDisconnected string = "disconnected" //断开连接
	kafkaProducerClosed       string = "closed"       //关闭
	DefaultKafkaAsyncProducer string = "default-kafka-async-producer"
	DefaultKafkaSyncProducer  string = "default-kafka-sync-producer"
)

type stdLogger interface {
	Print(v ...interface{})
	Printf(format string, v ...interface{})
	Println(v ...interface{})
}

var (
	ErrProduceTimeout   = errors.New("push message timeout")
	kafkaSyncProducers  = make(map[string]*SyncProducer)  //把生产者放入map中
	kafkaAsyncProducers = make(map[string]*AsyncProducer) //把生产者放入map中
	KafkaStdLogger      stdLogger
)

func init() {
	//初始化日志打印方法
	KafkaStdLogger = log.New(os.Stdout, "[kafka] ", log.LstdFlags|log.Lshortfile)
}

// 获取生产者的默认配置，clientID 唯一标识
func getDefaultProducerConfig(clientID string) (config *sarama.Config) {
	config = sarama.NewConfig()
	config.ClientID = clientID
	config.Version = sarama.V2_0_0_0

	//设置超时时间
	config.Net.DialTimeout = time.Second * 30
	config.Net.WriteTimeout = time.Second * 30
	config.Net.ReadTimeout = time.Second * 30

	//设置生产者重试间隔时间
	config.Producer.Retry.Backoff = time.Millisecond * 500
	//设置生产者重试次数
	config.Producer.Retry.Max = 3

	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	//需要小于broker的 `message.max.bytes`配置，默认是1000000
	config.Producer.MaxMessageBytes = 1000000 * 2

	config.Producer.RequiredAcks = sarama.WaitForLocal //acks 的值，需要确认收到kafka响应
	config.Producer.Partitioner = sarama.NewHashPartitioner

	//压缩比
	// zstd 算法有着最高的压缩比，而在吞吐量上的表现只能说中规中矩，LZ4 > Snappy > zstd 和 GZIP
	//LZ4 具有最高的吞吐性能，压缩比zstd > LZ4 > GZIP > Snappy
	//综上LZ4性价比最高
	config.Producer.Compression = sarama.CompressionLZ4
	return
}

// InitSyncKafkaProducer 初始化“同步生产者”,并保持连接状态keepconnect是开启的、以及检查程序运行后连接状态是关闭的则将状态改为closed
func InitSyncKafkaProducer(name string, hosts []string, config *sarama.Config) error {
	syncProducer := &SyncProducer{}
	syncProducer.Name = name
	syncProducer.Hosts = hosts
	syncProducer.Status = kafkaProducerDisconnected
	if config == nil {
		config = getDefaultProducerConfig(name)
	}
	syncProducer.Config = config

	//创建生产者实例
	if producer, err := sarama.NewSyncProducer(hosts, config); err != nil {
		return errors.Wrap(err, "NewSyncProducer err name "+name) // 返回自定义错误信息和 err
	} else {
		//有实例了，进行赋值
		syncProducer.Breaker = breaker.New(3, 1, 2*time.Second)
		syncProducer.ReConnect = make(chan bool)
		syncProducer.SyncProducer = &producer
		syncProducer.Status = kafkaProducerConnected
	}
	//保持连接状态
	go syncProducer.syncKeepConnect()
	//check连接状态
	go syncProducer.syncCheck()
	kafkaSyncProducers[name] = syncProducer
	return nil
}

// “同步生产者”保持连接状态是开启的
func (syncProducer *SyncProducer) syncKeepConnect() {
	defer func() {
		KafkaStdLogger.Println("syncProducer keepConnect exited")
	}()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	for {
		if syncProducer.Status == kafkaProducerClosed {
			return
		}
		select {
		case <-signals:
			syncProducer.StatusLock.Lock()
			syncProducer.Status = kafkaProducerClosed
			syncProducer.StatusLock.Unlock()
			return
		case <-syncProducer.ReConnect:
			if syncProducer.Status != kafkaProducerDisconnected {
				break
			}

			KafkaStdLogger.Println("kafka syncProducer ReConnecting... name " + syncProducer.Name)
			var producer sarama.SyncProducer
		syncBreakLoop:
			for {
				//利用熔断器给集群以恢复时间，避免不断的发送重联
				err := syncProducer.Breaker.Run(func() (err error) {
					producer, err = sarama.NewSyncProducer(syncProducer.Hosts, syncProducer.Config)
					return
				})

				switch err {
				case nil:
					syncProducer.StatusLock.Lock()
					if syncProducer.Status == kafkaProducerDisconnected {
						syncProducer.SyncProducer = &producer
						syncProducer.Status = kafkaProducerConnected
					}
					syncProducer.StatusLock.Unlock()
					KafkaStdLogger.Println("kafka syncProducer ReConnected, name:", syncProducer.Name)
					break syncBreakLoop
				case breaker.ErrBreakerOpen:
					KafkaStdLogger.Println("kafka connect fail, broker is open")
					//2s后重连，此时breaker刚好 half close
					if syncProducer.Status == kafkaProducerDisconnected {
						time.AfterFunc(2*time.Second, func() {
							KafkaStdLogger.Println("kafka begin to ReConnect ,because of  ErrBreakerOpen ")
							syncProducer.ReConnect <- true
						})
					}
					break syncBreakLoop
				default:
					KafkaStdLogger.Println("kafka ReConnect error, name:", syncProducer.Name, err)
				}
			}
		}
	}
}

// “同步生产者”检查在程序运行后连接状态是关闭则改为close
func (syncProducer *SyncProducer) syncCheck() {
	defer func() {
		KafkaStdLogger.Println("syncProducer check exited")
	}()
	// Trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	for {
		if syncProducer.Status == kafkaProducerClosed {
			return
		}
		select {
		case <-signals:
			syncProducer.StatusLock.Lock()
			//if (*syncProducer.SyncProducer) != nil {
			//	err := (*syncProducer.SyncProducer).Close()
			//	if err != nil {
			//		KafkaStdLogger.Println("kafka syncProducer close error", err)
			//	}
			//}
			syncProducer.Status = kafkaProducerClosed
			syncProducer.StatusLock.Unlock()
			return
		}
	}
}

// NewKafkaSyncProducer 创建“同步生产者”的实例，外界可以调用，这样就可以调用实例的方法如send，sendmessage
func NewKafkaSyncProducer(name string) *SyncProducer {
	if producer, ok := kafkaSyncProducers[name]; ok {
		return producer
	} else {
		KafkaStdLogger.Println("InitSyncKafkaProducer must be called !")
		return nil
	}
}

// SyncSend 发送“同步生产者”Send消息 sarama.producermessage 里面包含有topic
func (syncProducer *SyncProducer) SyncSend(msg *sarama.ProducerMessage) (partition int32, offset int64, err error) {
	if syncProducer.Status != kafkaProducerConnected {
		return -1, -1, errors.New("kafka syncProducer " + syncProducer.Status)
	}
	partition, offset, err = (*syncProducer.SyncProducer).SendMessage(msg)
	if err == nil {
		return
	}
	if errors.Is(err, sarama.ErrBrokerNotAvailable) {
		syncProducer.StatusLock.Lock()
		if syncProducer.Status == kafkaProducerConnected {
			syncProducer.Status = kafkaProducerDisconnected
			syncProducer.ReConnect <- true //重连
		}
		syncProducer.StatusLock.Unlock()
	}
	return
}

// SyncSendMessages 发送“同步生产者”多条SendMessages
func (syncProducer *SyncProducer) SyncSendMessages(mses []*sarama.ProducerMessage) (errs sarama.ProducerErrors) {
	if syncProducer.Status != kafkaProducerConnected {
		return append(errs, &sarama.ProducerError{Err: errors.New("kafka syncProducer " + syncProducer.Status)})
	}
	errs = (*syncProducer.SyncProducer).SendMessages(mses).(sarama.ProducerErrors)
	for _, err := range errs {
		//触发重连
		if errors.Is(err, sarama.ErrBrokerNotAvailable) { //判断kafka是否可以连上
			syncProducer.StatusLock.Lock()
			if syncProducer.Status == kafkaProducerConnected {
				syncProducer.Status = kafkaProducerDisconnected
				syncProducer.ReConnect <- true
			}
			syncProducer.StatusLock.Unlock()
		}
	}
	return
}

// InitAsyncKafkaProducer 初始化“异步生产者”,并保持连接状态keepconnect是开启的、以及检查连接状态是关闭的则将状态改为closed
func InitAsyncKafkaProducer(name string, hosts []string, config *sarama.Config) error {
	asyncProducer := &AsyncProducer{}
	asyncProducer.Name = name
	asyncProducer.Hosts = hosts
	asyncProducer.Status = kafkaProducerDisconnected
	if config == nil {
		config = getDefaultProducerConfig(name)
	}
	asyncProducer.Config = config

	if producer, err := sarama.NewAsyncProducer(hosts, config); err != nil {
		return errors.Wrap(err, "NewAsyncProducer error name"+name)
	} else {

		asyncProducer.Breaker = breaker.New(3, 1, 5*time.Second)
		asyncProducer.ReConnect = make(chan bool)
		asyncProducer.AsyncProducer = &producer
		asyncProducer.Status = kafkaProducerConnected
		KafkaStdLogger.Println("AsyncKakfaProducer  connected name ", name)
	}

	go asyncProducer.AsyncKeepConnect()
	go asyncProducer.AsyncCheck()
	kafkaAsyncProducers[name] = asyncProducer
	return nil
}

// AsyncSend 异步发送消息到 kafka
func (asyncProducer *AsyncProducer) AsyncSend(msg *sarama.ProducerMessage) error {
	var err error
	if asyncProducer.Status != kafkaProducerConnected {
		return errors.New("kafka disconnected")
	}
	(*asyncProducer.AsyncProducer).Input() <- msg //发送到通道中，然后SDK里面会定时把消息发送到kafka里面，批量操作
	//select {
	//case (*asyncProducer.AsyncProducer).Input() <- msg:
	//case <-time.After(5 * time.Second):
	//	err = ErrProduceTimeout
	//	// retry
	//	select {
	//	case (*asyncProducer.AsyncProducer).Input() <- msg:
	//		err = nil
	//	default:
	//	}
	//
	//}
	return err
}

// NewKafkaAsyncProducer 创建“异步生产者”的实例，外界可以调用，这样就可以调用实例的方法如send
func NewKafkaAsyncProducer(name string) *AsyncProducer {
	if producer, ok := kafkaAsyncProducers[name]; ok {
		return producer
	} else {
		KafkaStdLogger.Println("InitAsyncKafkaProducer must be called !")
		return nil
	}
}

// AsyncKeepConnect “异步生产者”保持连接状态是开启的
func (asyncProducer *AsyncProducer) AsyncKeepConnect() {
	defer func() {
		KafkaStdLogger.Println("asyncProducer keepConnect exited")
	}()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	for {
		if asyncProducer.Status == kafkaProducerClosed {
			return
		}
		select {
		case s := <-signals:
			KafkaStdLogger.Println("kafka async producer receive system signal:" + s.String() + "; name:" + asyncProducer.Name)
			//if (*asyncProducer.AsyncProducer) != nil {
			//	err := (*asyncProducer.AsyncProducer).Close()
			//	if err != nil {
			//		KafkaStdLogger.Println("kafka syncProducer close error", zap.Error(err))
			//	}
			//}
			asyncProducer.Status = kafkaProducerClosed
			return
		case <-asyncProducer.ReConnect:
			if asyncProducer.Status != kafkaProducerDisconnected {
				break
			}

			KafkaStdLogger.Println("kafka syncProducer ReConnecting... name" + asyncProducer.Name)
			var producer sarama.AsyncProducer
		asyncBreakLoop:
			for {
				//利用熔断器给集群以恢复时间，避免不断的发送重联
				err := asyncProducer.Breaker.Run(func() (err error) {
					producer, err = sarama.NewAsyncProducer(asyncProducer.Hosts, asyncProducer.Config)
					return
				})

				switch err {
				case nil:
					asyncProducer.StatusLock.Lock()
					if asyncProducer.Status == kafkaProducerDisconnected {
						asyncProducer.AsyncProducer = &producer
						asyncProducer.Status = kafkaProducerConnected
					}
					asyncProducer.StatusLock.Unlock()
					//logger.Info("kafka syncProducer ReConnected, name:" + asyncProducer.Name)
					break asyncBreakLoop
				case breaker.ErrBreakerOpen:
					KafkaStdLogger.Println("kafka connect fail, broker is open")
					//2s后重连，此时breaker刚好 half close
					if asyncProducer.Status == kafkaProducerDisconnected {
						time.AfterFunc(2*time.Second, func() {
							KafkaStdLogger.Println("kafka begin to ReConnect ,because of  ErrBreakerOpen ")
							asyncProducer.ReConnect <- true
						})
					}
					break asyncBreakLoop
				default:
					KafkaStdLogger.Println("kafka ReConnect error, name:"+asyncProducer.Name, zap.Error(err))
				}
			}
		}
	}
}

// AsyncCheck “异步生产者”检查在程序运行后连接状态是关闭则改为close
func (asyncProducer *AsyncProducer) AsyncCheck() {
	defer func() {
		KafkaStdLogger.Println("asyncProducer check exited")
	}()
	for {
		switch asyncProducer.Status {
		case kafkaProducerDisconnected:
			time.Sleep(time.Second * 5)
			continue
		case kafkaProducerClosed:
			return
		}
		// Trap SIGINT to trigger a shutdown.
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

		for {
			select {
			case msg := <-(*asyncProducer.AsyncProducer).Successes():
				logger.Info("Success produce message  ", zap.Any(msg.Topic, msg.Value))
			case err := <-(*asyncProducer.AsyncProducer).Errors():
				KafkaStdLogger.Println("message send error", zap.Error(err))
				if errors.Is(err, sarama.ErrOutOfBrokers) || errors.Is(err, sarama.ErrNotConnected) {
					// 连接中断触发重连，捕捉不到 EOF
					asyncProducer.StatusLock.Lock()
					if asyncProducer.Status == kafkaProducerConnected {
						asyncProducer.Status = kafkaProducerDisconnected
						asyncProducer.ReConnect <- true
					}
					asyncProducer.StatusLock.Unlock()
				}
			case s := <-signals:
				KafkaStdLogger.Println("kafka async producer receive system signal:" + s.String() + "; name:" + asyncProducer.Name)
				//if (*asyncProducer.AsyncProducer) != nil {
				//	err := (*asyncProducer.AsyncProducer).Close()
				//	if err != nil {
				//		KafkaStdLogger.Println("kafka syncProducer close error", zap.Error(err))
				//	}
				//}
				asyncProducer.Status = kafkaProducerClosed
				return
			}
		}
	}
}

// Close 关闭“同步生产者”
func (syncProducer *SyncProducer) Close() error {
	syncProducer.StatusLock.Lock()
	defer syncProducer.StatusLock.Unlock()

	err := (*syncProducer.SyncProducer).Close()
	syncProducer.Status = kafkaProducerClosed
	return err

}

// Close 关闭“异步生产者”
func (asyncProducer *AsyncProducer) Close() error {
	asyncProducer.StatusLock.Lock()
	defer asyncProducer.StatusLock.Unlock()
	err := (*asyncProducer.AsyncProducer).Close()
	asyncProducer.Status = kafkaProducerClosed
	return err
}

// KafkaMsgValueEncoder 发送消息格式化
func KafkaMsgValueEncoder(value []byte) sarama.Encoder {
	return sarama.ByteEncoder(value)
}
