package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"

	"encoding/json"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
)

type Config struct {
	host       string
	kafka_host string
	topic      string
}

type App struct {
	producer *kafka.Producer
	config   Config
}

func main() {
	c := Config{
		host:       getenv("WRITE_ADAPTOR_HOST", ":8080"),
		kafka_host: getenv("KAKFA_BOOTSTRAP_SERVER", "localhost"),
		topic:      getenv("KAFKA_TOPIC", "prometheus"),
	}

	p, err := setup_kafka_producer(c)
	if err != nil {
		fmt.Println("error: ", err)
		log.Fatal("Failed to connect to kafka")
		os.Exit(1)
	}

	app := App{
		producer: p,
		config:   c,
	}

	http_receive(app)
}

func getenv(key, fallback string) string {
	value := os.Getenv(key)
	if len(value) == 0 {
		return fallback
	}
	return value
}

func http_receive(app App) {
	http.HandleFunc("/receive", func(w http.ResponseWriter, r *http.Request) {
		compressed, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		reqBuf, err := snappy.Decode(nil, compressed)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		var req prompb.WriteRequest
		if err := proto.Unmarshal(reqBuf, &req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		err = handle_request(app, req)

		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	})

	log.Fatal(http.ListenAndServe(app.config.host, nil))
}

func setup_kafka_producer(config Config) (*kafka.Producer, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": config.kafka_host})
	return p, err
}

func handle_request(app App, req prompb.WriteRequest) error {
	var err error
	for _, ts := range req.Timeseries {
		m := make(model.Metric, len(ts.Labels))
		for _, l := range ts.Labels {
			m[model.LabelName(l.Name)] = model.LabelValue(l.Value)
		}

		b, e := json.Marshal(m)
		if e != nil {
			err = e
			fmt.Println("error: ", err)
			break
		}

		err = send(app, b)
		if err != nil {
			fmt.Println("error: ", err)
			break
		}
	}

	return err
}

func send(app App, json_bytes []byte) error {
	topic := app.config.topic
	msg := kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          json_bytes,
	}

	return send_to_topic(app, msg)
}

func send_to_topic(app App, msg kafka.Message) error {
	err := app.producer.Produce(&msg, nil)
	return err
}
