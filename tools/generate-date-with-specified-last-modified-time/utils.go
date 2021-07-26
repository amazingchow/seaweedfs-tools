package main

import (
	"encoding/json"
	"io/ioutil"
	"time"

	"github.com/sirupsen/logrus"
)

type TimeResponse struct {
	Status    string `json:"status"`
	Timestamp int64  `json:"timestamp"`
}

func (scanner *VolumeFileScanner4Transformer) FetchFakeTimestamp() (int64, error) {
	resp, err := scanner.httpClient.Get("http://127.0.0.1:5000/random_timestamp?passed=90")
	if err != nil {
		logrus.WithError(err).Error("failed to fetch fake timestamp")
		return time.Now().Unix(), err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		logrus.WithError(err).Error("failed to read fake timestamp")
		return time.Now().Unix(), err
	}

	var tr TimeResponse
	if err = json.Unmarshal(body, &tr); err != nil {
		logrus.WithError(err).Error("failed to unmarshal fake timestamp")
		return time.Now().Unix(), err
	}
	return tr.Timestamp, nil
}
