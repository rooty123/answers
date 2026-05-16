package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/go-pg/pg/v10"
	"github.com/go-pg/pg/v10/orm"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Answer struct {
	tableName  struct{}  `pg:"answers"`
	ID         int64     `json:"id" pg:"id,pk"`
	ChatID     int64     `json:"chat_id" pg:"chat_id,use_zero"`
	TelegramID int64     `json:"telegram_id" pg:"telegram_id,use_zero"`
	Text       string    `json:"text" pg:"text"`
	SentAt     time.Time `json:"sent_at" pg:"sent_at"`
	CreatedAt  time.Time `json:"created_at,omitempty" pg:"created_at,default:now()"`
}

type postReq struct {
	ChatID     int64     `json:"chat_id"`
	TelegramID int64     `json:"telegram_id"`
	Text       string    `json:"text"`
	SentAt     time.Time `json:"sent_at"`
}

type kafkaEvent struct {
	ChatID   int64     `json:"chat_id"`
	AnswerID int64     `json:"answer_id"`
	SentAt   time.Time `json:"sent_at"`
}

var (
	log      *logrus.Entry
	db       *pg.DB
	producer *kgo.Client
	topic    = "answers.received"

	httpReqs = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "answers_http_requests_total",
		Help: "HTTP requests by route and status",
	}, []string{"route", "status"})
)

func initLogger() {
	l := logrus.New()
	l.SetFormatter(&logrus.JSONFormatter{})
	if lvl, err := logrus.ParseLevel(os.Getenv("LOG_LEVEL")); err == nil {
		l.SetLevel(lvl)
	}
	name := os.Getenv("SERVICE_NAME")
	if name == "" {
		name = "answers"
	}
	log = l.WithField("service_name", name)
}

func initDB() {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		log.Fatal("DATABASE_URL is not set")
	}
	opts, err := pg.ParseURL(dsn)
	if err != nil {
		log.WithError(err).Fatal("Invalid DATABASE_URL")
	}
	db = pg.Connect(opts)
	if err := db.Ping(context.Background()); err != nil {
		log.WithError(err).Fatal("Cannot ping database")
	}
	if err := db.Model((*Answer)(nil)).CreateTable(&orm.CreateTableOptions{IfNotExists: true}); err != nil {
		log.WithError(err).Fatal("CreateTable failed")
	}
	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS answers_chat_id_sent_at_idx ON answers (chat_id, sent_at DESC)`); err != nil {
		log.WithError(err).Fatal("Create index failed")
	}
}

func initKafka() {
	boot := os.Getenv("KAFKA_BOOTSTRAP")
	if boot == "" {
		log.Fatal("KAFKA_BOOTSTRAP is not set")
	}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(boot, ",")...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(10*time.Millisecond),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		log.WithError(err).Fatal("Kafka client init failed")
	}
	producer = cl
}

func postAnswer(c echo.Context) error {
	var req postReq
	if err := c.Bind(&req); err != nil {
		httpReqs.WithLabelValues("POST /answers", "400").Inc()
		return c.JSON(http.StatusBadRequest, echo.Map{"error": "invalid body"})
	}
	if req.ChatID == 0 || req.Text == "" {
		httpReqs.WithLabelValues("POST /answers", "400").Inc()
		return c.JSON(http.StatusBadRequest, echo.Map{"error": "chat_id and text are required"})
	}
	if req.SentAt.IsZero() {
		req.SentAt = time.Now().UTC()
	}

	a := &Answer{
		ChatID:     req.ChatID,
		TelegramID: req.TelegramID,
		Text:       req.Text,
		SentAt:     req.SentAt,
	}
	if _, err := db.Model(a).Returning("id, created_at").Insert(); err != nil {
		log.WithError(err).Error("DB insert failed")
		httpReqs.WithLabelValues("POST /answers", "500").Inc()
		return c.JSON(http.StatusInternalServerError, echo.Map{"error": "db insert failed"})
	}

	event := kafkaEvent{ChatID: a.ChatID, AnswerID: a.ID, SentAt: a.SentAt}
	payload, _ := json.Marshal(event)
	rec := &kgo.Record{
		Topic: topic,
		Key:   []byte(strconv.FormatInt(a.ChatID, 10)),
		Value: payload,
	}
	ctx, cancel := context.WithTimeout(c.Request().Context(), 3*time.Second)
	defer cancel()
	if err := producer.ProduceSync(ctx, rec).FirstErr(); err != nil {
		log.WithError(err).WithField("answer_id", a.ID).Error("Kafka publish failed")
		httpReqs.WithLabelValues("POST /answers", "500").Inc()
		return c.JSON(http.StatusInternalServerError, echo.Map{"error": "kafka publish failed"})
	}

	log.WithFields(logrus.Fields{"answer_id": a.ID, "chat_id": a.ChatID}).Info("Answer stored")
	httpReqs.WithLabelValues("POST /answers", "201").Inc()
	return c.JSON(http.StatusCreated, a)
}

func getAnswers(c echo.Context) error {
	chatIDStr := c.QueryParam("chat_id")
	if chatIDStr == "" {
		return c.JSON(http.StatusBadRequest, echo.Map{"error": "chat_id is required"})
	}
	chatID, err := strconv.ParseInt(chatIDStr, 10, 64)
	if err != nil {
		return c.JSON(http.StatusBadRequest, echo.Map{"error": "invalid chat_id"})
	}
	limit := 50
	if l := c.QueryParam("limit"); l != "" {
		if v, err := strconv.Atoi(l); err == nil && v > 0 && v <= 500 {
			limit = v
		}
	}
	var rows []Answer
	if err := db.Model(&rows).Where("chat_id = ?", chatID).Order("sent_at DESC").Limit(limit).Select(); err != nil {
		log.WithError(err).Error("DB select failed")
		return c.JSON(http.StatusInternalServerError, echo.Map{"error": "db select failed"})
	}
	httpReqs.WithLabelValues("GET /answers", "200").Inc()
	return c.JSON(http.StatusOK, rows)
}

func health(c echo.Context) error {
	return c.String(http.StatusOK, "ok")
}

func main() {
	initLogger()
	initDB()
	initKafka()

	e := echo.New()
	e.HideBanner = true
	e.Use(middleware.Recover())

	e.POST("/answers", postAnswer)
	e.GET("/answers", getAnswers)
	e.GET("/healthz", health)
	e.GET("/metrics", echo.WrapHandler(promhttp.Handler()))

	port := os.Getenv("SERVER_PORT")
	if port == "" {
		port = "8080"
	}

	srvCh := make(chan error, 1)
	go func() {
		log.WithField("port", port).Info("HTTP server starting")
		if err := e.Start(":" + port); err != nil && !errors.Is(err, http.ErrServerClosed) {
			srvCh <- err
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	select {
	case err := <-srvCh:
		log.WithError(err).Error("HTTP server failed")
	case sig := <-sigCh:
		log.WithField("signal", sig.String()).Info("Gracefully shutting down")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := e.Shutdown(ctx); err != nil {
		log.WithError(err).Warn("HTTP shutdown error")
	}
	if err := producer.Flush(ctx); err != nil {
		log.WithError(err).Warn("Kafka flush error")
	}
	producer.Close()
	if err := db.Close(); err != nil {
		log.WithError(err).Warn("DB close error")
	}
	fmt.Fprintln(os.Stdout, "Gracefully shutting down")
	os.Exit(0)
}
