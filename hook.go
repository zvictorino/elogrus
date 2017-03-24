package elogrus

import (
	"fmt"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"

	"golang.org/x/net/context"
	"gopkg.in/olivere/elastic.v5"
)

var (
	// Fired if the
	// index is not created
	ErrCannotCreateIndex = fmt.Errorf("Cannot create index")
)

type IndexNameFunc func() string

type IndexCleanUpFunc func(map[string]bool)

// ElasticHook is a logrus
// hook for ElasticSearch
type ElasticHook struct {
	client           *elastic.Client
	host             string
	index            IndexNameFunc
	indexCleanUpFunc IndexCleanUpFunc
	createdIdx       map[string]bool
	levels           []logrus.Level
	ctx              context.Context
	ctxCancel        context.CancelFunc
}

// NewElasticHook creates new hook
// client - ElasticSearch client using gopkg.in/olivere/elastic.v5
// host - host of system
// level - log level
// index - name of the index in ElasticSearch
func NewElasticHook(client *elastic.Client, host string, level logrus.Level, index string) (*ElasticHook, error) {
	return NewElasticHookWithFunc(client, host, level, func() string { return index }, func(map[string]bool) {})
}

// NewElasticHook creates new hook
// client - ElasticSearch client using gopkg.in/olivere/elastic.v5
// host - host of system
// level - log level
// indexFunc - function to return index name
// cleanUp - the function is called after each index creation,
//			to provide ability to cleanup old entries
func NewElasticHookWithFunc(client *elastic.Client, host string, level logrus.Level, indexFunc IndexNameFunc, cleanUp IndexCleanUpFunc) (*ElasticHook, error) {
	levels := []logrus.Level{}
	for _, l := range []logrus.Level{
		logrus.PanicLevel,
		logrus.FatalLevel,
		logrus.ErrorLevel,
		logrus.WarnLevel,
		logrus.InfoLevel,
		logrus.DebugLevel,
	} {
		if l <= level {
			levels = append(levels, l)
		}
	}

	ctx, cancel := context.WithCancel(context.TODO())

	hook := &ElasticHook{
		client:           client,
		host:             host,
		index:            indexFunc,
		indexCleanUpFunc: cleanUp,
		createdIdx:       map[string]bool{},
		levels:           levels,
		ctx:              ctx,
		ctxCancel:        cancel,
	}

	if _, err := hook.getIndex(); err != nil {
		return nil, err
	}

	return hook, nil
}

// Fire is required to implement
// Logrus hook
func (hook *ElasticHook) Fire(entry *logrus.Entry) error {

	idx, err := hook.getIndex()
	if err != nil {
		return err
	}

	level := entry.Level.String()

	msg := struct {
		Host      string
		Timestamp string
		Message   string
		Data      logrus.Fields
		Level     string
	}{
		hook.host,
		entry.Time.UTC().Format(time.RFC3339Nano),
		entry.Message,
		entry.Data,
		strings.ToUpper(level),
	}

	_, err = hook.client.
		Index().
		Index(idx).
		Type("log").
		BodyJson(msg).
		Do(hook.ctx)

	return err
}

// Required for logrus
// hook implementation
func (hook *ElasticHook) Levels() []logrus.Level {
	return hook.levels
}

// Cancels all calls to
// elastic
func (hook *ElasticHook) Cancel() {
	hook.ctxCancel()
}

func (hook *ElasticHook) getIndex() (string, error) {
	idx := hook.index()
	if !hook.createdIdx[idx] {
		// Use the IndexExists service to check if a specified index exists.
		exists, err := hook.client.IndexExists(idx).Do(hook.ctx)
		if err != nil {
			// Handle error
			return "", err
		}
		if !exists {
			createIndex, err := hook.client.CreateIndex(idx).Do(hook.ctx)
			if err != nil {
				return "", err
			}
			if !createIndex.Acknowledged {
				return "", ErrCannotCreateIndex
			}
		}
		hook.createdIdx[idx] = true
		hook.indexCleanUpFunc(hook.createdIdx)
	}
	return idx, nil
}
