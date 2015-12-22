package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"os"
	"os/signal"
	"syscall"
)

/*
A Consumer holds the channel and connection for a rabbit worker.
*/
type Spaceballz struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	tag     string
	done    chan error
	Work    func(delivery amqp.Delivery) error
}

func (s Spaceballz) Conn() *amqp.Connection {
	return s.conn
}

func (s Spaceballz) Channel() *amqp.Channel {
	return s.channel
}

/*
A rabbit worker can take a delivery and return an error.
Workers should Ack on successful processing.
Reject if message is conflicting with state (ie duplicate key).
Nack if there is a dependency failure.
*/
type Worker interface {
	Work(delivery amqp.Delivery) error
}

func NewBlower(amqpURI string) (*Spaceballz, error) {
	c := &Spaceballz{
		conn:    nil,
		channel: nil,
	}

	var err error

	log.Printf("dialing %q", amqpURI)
	c.conn, err = amqp.Dial(amqpURI)
	if err != nil {
		return nil, fmt.Errorf("Dial: %s", err)
	}

	go func() {
		fmt.Printf("closing: %s", <-c.conn.NotifyClose(make(chan *amqp.Error)))
	}()

	log.Printf("got Connection, getting Channel")
	c.channel, err = c.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("Channel: %s", err)
	}

	return c, nil
}

/*
Creates a `Consumer` type with exchange,queue and bindings declared with a worker function ready to roll.
*/
func NewSucker(amqpURI, queueName, ctag string, work func(delivery amqp.Delivery) error) (*Spaceballz, error) {
	c := &Spaceballz{
		conn:    nil,
		channel: nil,
		tag:     ctag,
		done:    make(chan error),
		Work:    work,
	}

	var err error

	log.Printf("dialing %q", amqpURI)
	c.conn, err = amqp.Dial(amqpURI)
	if err != nil {
		return nil, fmt.Errorf("Dial: %s", err)
	}

	go func() {
		fmt.Printf("closing: %s", <-c.conn.NotifyClose(make(chan *amqp.Error)))
	}()

	log.Printf("got Connection, getting Channel")
	c.channel, err = c.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("Channel: %s", err)
	}

	deliveries, err := c.channel.Consume(
		queueName, // name
		c.tag,     // consumerTag,
		false,     // noAck
		false,     // exclusive
		false,     // noLocal
		false,     // noWait
		nil,       // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("Queue Consume: %s", err)
	}

	go c.handle(deliveries, c.done)

	return c, nil
}

func (c *Spaceballz) Shutdown() error {
	// will close() the deliveries channel
	if err := c.channel.Cancel(c.tag, true); err != nil {
		return fmt.Errorf("Consumer cancel failed: %s", err)
	}

	if err := c.conn.Close(); err != nil {
		return fmt.Errorf("AMQP connection close error: %s", err)
	}

	defer log.Printf("AMQP shutdown OK")

	// wait for handle() to exit
	return <-c.done
}

func (c *Spaceballz) handle(deliveries <-chan amqp.Delivery, done chan error) {
	for d := range deliveries {
		c.Work(d)
	}
	log.Printf("handle: deliveries channel closed")
	done <- nil
}

func Suck(uri string, queue string) {
	_, err := NewSucker(uri, queue, "spaceballz", func(delivery amqp.Delivery) error {
		fmt.Println(string(delivery.Body))
		delivery.Ack(false)
		return nil
	})

	if nil != err {
		log.Fatal(err)
	}

	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)
	log.Println(<-ch)
}

func Blow(uri string, queue string, file string) {
	b, err := NewBlower(uri)
	if err != nil {
		log.Fatalf("Had some error %s", err)
	}

	log.Printf("blower instantiated")

	inFile, err := os.Open(file)
	if err != nil {
		log.Fatalf("Had some error %s", err)
	}

	defer inFile.Close()
	scanner := bufio.NewScanner(inFile)
	scanner.Split(bufio.ScanLines)

	log.Printf("Publishing to queue %s \n", queue)
	for scanner.Scan() {
		log.Printf("Publishing %s \n", scanner.Text())
		b.Channel().Publish(
			"",
			queue,
			false,
			false,
			amqp.Publishing{
				Headers:         amqp.Table{},
				ContentType:     "text/plain",
				ContentEncoding: "",
				Body:            scanner.Bytes(),
				DeliveryMode:    amqp.Persistent,
				Priority:        0,
			},
		)
	}

	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)
	log.Println(<-ch)
}

func main() {
	flag.Usage = func() {
		fmt.Println("spaceballz -action=suck -uri=<amqp-uri> -queue=<queue>")
		fmt.Println("spaceballz -action=blow -uri=<amqp-uri> -queue=<queue> file1 file2 file3")
	}
	uriFlag := flag.String("uri", "", "host to connect to")
	queueFlag := flag.String("queue", "", "queue to connect to")
	actionFlag := flag.String("action", "", "`suck` to consume, `blow` to publish.")

	flag.Parse()

	if *actionFlag == "suck" {
		Suck(*uriFlag, *queueFlag)
	} else if *actionFlag == "blow" {
		Blow(*uriFlag, *queueFlag, flag.Args()[0])
	} else {
		log.Fatal("Gotta suck or blow")
	}
}
