package main

import (
	"github.com/cookingkode/nsq_forsure"
	"github.com/nsqio/go-nsq"
	"log"
	"sync"
	"time"
)

func do_produce(wg *sync.WaitGroup) {
	//config := nsq.NewConfig()
	w, err := nsqForSure.NewProducer("127.0.0.1:4150", "127.0.0.1:6379")

	if err != nil {
		log.Panic("error in NewProducer", err)
	}

	err = w.Publish("write_test", "key_test", 10000, []byte("test is this"))
	if err != nil {
		log.Panic("Could not connect")
	}

	wg.Done()
	//w.Stop()
}

func main() {
	wg := &sync.WaitGroup{}
	wg.Add(2)
	do_produce(wg)

	time.Sleep(10 * time.Second)

	q, _ := nsqForSure.NewConsumer("write_test", "ch", "127.0.0.1:6379", 100000, 10)
	q.AddConcurrentHandlers(nsq.HandlerFunc(func(message *nsq.Message) error {
		log.Printf("Got a message: %v\n", string(message.Body[:]))
		wg.Done()
		return nil
	}))
	err := q.ConnectToNSQD("127.0.0.1:4150")
	if err != nil {
		log.Panic("Could not connect")
	}

	wg.Wait()
}
