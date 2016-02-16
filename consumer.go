package nsqForSure

import (
	"bytes"
	"encoding/gob"
	"github.com/garyburd/redigo/redis"
	"github.com/nsqio/go-nsq"
	"log"
)

type Consumer struct {
	consumerH *nsq.Consumer
	handler   nsq.Handler
	redisPool *redis.Pool
	etaMsec   int
}

func NewConsumer(topic string, channel string, redisAddress string, etaMsec int, concurrency int) (*Consumer, error) {

	cCfg := nsq.NewConfig()
	consumerH, err := nsq.NewConsumer(topic, channel, cCfg)
	if err != nil {
		log.Printf("error in NewConsumer &v", err)
		return nil, err
	}

	consumer := &Consumer{
		consumerH: consumerH,
		redisPool: newPool(redisAddress, ""),
		etaMsec:   etaMsec,
		handler:   nil,
	}

	consumerH.AddConcurrentHandlers(consumer, concurrency)
	return consumer, err
}

func (c *Consumer) AddConcurrentHandlers(handler nsq.Handler) {
	c.handler = handler
}

func (c *Consumer) ConnectToNSQD(addr string) error {
	return c.consumerH.ConnectToNSQD(addr)
}

func (c *Consumer) ConnectToNSQDs(addr []string) error {
	return c.consumerH.ConnectToNSQDs(addr)
}

func (c *Consumer) ConnectToNSQLookupd(addr string) error {
	return c.consumerH.ConnectToNSQLookupd(addr)
}

func (c *Consumer) HandleMessage(message *nsq.Message) error {
	var msg keyedMessage

	//1. Decode message
	dec := gob.NewDecoder(bytes.NewBuffer(message.Body)) // should read from nw ideally TODO
	err := dec.Decode(&msg)
	if err != nil {
		log.Printf("decode error : %v", err)
	}

	conn := c.redisPool.Get()
	defer conn.Close()

	log.Printf("NSQ for sure MainHandler got a message key:%v body%v\n", msg.Key, msg.Body)

	//2. Change Key to INWORK with ETA
	resp, err := conn.Do("SET", msg.Key, "INWORK", "xx", "px", c.etaMsec)
	if err != nil || resp != "OK" {
		log.Printf("consumer key set failure  %v %v", resp, err)
	}

	//3. Do actual work
	if c.handler != nil {
		message.Body = msg.Body // replace with contained msg
		c.handler.HandleMessage(message)
	} else {
		log.Printf("Nil Handler\n")
	}
	//4. FINISH WORK
	resp, err = conn.Do("SET", msg.Key, "FINISH", "xx")
	if err != nil || resp != "OK" {
		log.Printf("consumer key set to finish failure  %v %v", resp, err)
	}

	return nil

}
