package nsqForSure

import (
	"bytes"
	"encoding/gob"
	"errors"
	"github.com/garyburd/redigo/redis"
	"github.com/nsqio/go-nsq"
	"log"
	"time"
)

type Producer struct {
	masterH   *nsq.Producer
	slaveH    *nsq.Producer
	mAddr     string
	sAddr     string
	redisPool *redis.Pool
}

var (
	taskAlreadyUnderProgressError = errors.New("Task already in progress")
)

func NewProducer(nsqdAddr string, redisAddr string) (*Producer, error) {

	// TODO slave := findSlave(nsqdAddr)
	slave := ""

	pCfg := nsq.NewConfig()
	masterProducer, err := nsq.NewProducer(nsqdAddr, pCfg)
	if err != nil {
		return nil, err
	}

	slaveProducer := (*nsq.Producer)(nil)

	sCfg := nsq.NewConfig()
	if len(slave) > 0 {
		slaveProducer, err = nsq.NewProducer(slave, sCfg)
		if err != nil {
			return nil, err
		}
	}

	pH := Producer{
		masterH:   masterProducer,
		slaveH:    slaveProducer,
		mAddr:     nsqdAddr,
		sAddr:     slave,
		redisPool: newPool(redisAddr, ""),
	}

	return &pH, nil
}

func (w *Producer) Publish(topic, key string, ttlMsec int64, body []byte) error {

	conn := w.redisPool.Get()
	defer conn.Close()

	//1. Check key not exists, and set with TTL
	resp, err := conn.Do("SET", key, "INPROGRESS", "nx", "px", ttlMsec)
	if err != nil || resp != "OK" {
		log.Printf("producer key set failure  %v %v", resp, err)
		return taskAlreadyUnderProgressError
	}

	//2. Encode
	//  Normally enc would be bound to network connections TODO
	var network bytes.Buffer        // Stand-in for a network connection
	enc := gob.NewEncoder(&network) // Will write to network.

	err = enc.Encode(keyedMessage{
		Key:       key,
		KeyLength: int64(len(key)),
		Body:      body,
	})
	if err != nil {
		log.Printf("serialization failure %v", err)
		return err
	}

	toBeSent := network.Bytes()

	//3. Send the message
	err = w.masterH.Publish(topic, toBeSent)
	if err != nil {
		log.Printf("Master push failure %v", err)
		return err
	}

	if w.slaveH != nil {
		err = w.slaveH.Publish(topic, toBeSent)
		if err != nil {
			log.Printf("Slave push failure %v", err)
		}
	}

	//4. Check for err
	if err != nil {
		//5. delete the key
		conn.Do("DEL", key)
	}

	return nil
}

func newPool(server, password string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", server)
			if err != nil {
				return nil, err
			}

			if password != "" {
				if _, err := c.Do("AUTH", password); err != nil {
					c.Close()
					return nil, err
				}
			}

			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}
