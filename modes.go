package main

import (
	"fmt"
	"log"
	"strings"
	"sync/atomic"
	"time"

	"github.com/codahale/hdrhistogram"
	"github.com/gocql/gocql"
)

type RateLimiter interface {
	Wait()
	ExpectedInterval() int64
}

type UnlimitedRateLimiter struct{}

func (*UnlimitedRateLimiter) Wait() {}

func (*UnlimitedRateLimiter) ExpectedInterval() int64 {
	return 0
}

type MaximumRateLimiter struct {
	Period      time.Duration
	LastRequest time.Time
}

func (mxrl *MaximumRateLimiter) Wait() {
	nextRequest := mxrl.LastRequest.Add(mxrl.Period)
	now := time.Now()
	if now.Before(nextRequest) {
		time.Sleep(nextRequest.Sub(now))
	}
	mxrl.LastRequest = time.Now()
}

func (mxrl *MaximumRateLimiter) ExpectedInterval() int64 {
	return mxrl.Period.Nanoseconds()
}

func NewRateLimiter(maximumRate int, timeOffset time.Duration) RateLimiter {
	if maximumRate == 0 {
		return &UnlimitedRateLimiter{}
	}
	period := time.Duration(int64(time.Second) / int64(maximumRate))
	lastRequest := time.Now().Add(timeOffset)
	return &MaximumRateLimiter{period, lastRequest}
}

type Result struct {
	ElapsedTime    time.Duration
	Operations     int
	ClusteringRows int
	Latency        *hdrhistogram.Histogram
}

type MergedResult struct {
	Time                    time.Duration
	Operations              int
	ClusteringRows          int
	OperationsPerSecond     float64
	ClusteringRowsPerSecond float64
	Latency                 *hdrhistogram.Histogram
}

func NewHistogram() *hdrhistogram.Histogram {
	return hdrhistogram.New(time.Microsecond.Nanoseconds()*50, (timeout + timeout*2).Nanoseconds(), 3)
}

var reportedError uint32

func HandleError(err error) {
	if atomic.SwapUint32(&stopAll, 1) == 0 {
		log.Print(err)
		fmt.Println("\nstopping")
		atomic.StoreUint32(&stopAll, 1)
	}
}

func RunConcurrently(maximumRate int, workload func(id int, rateLimiter RateLimiter) Result) MergedResult {
	var timeOffsetUnit int64
	if maximumRate != 0 {
		timeOffsetUnit = int64(time.Second) / int64(maximumRate)
		maximumRate /= concurrency
	} else {
		timeOffsetUnit = 0
	}

	results := make([]chan Result, concurrency)
	for i := range results {
		results[i] = make(chan Result, 1)
	}

	for i := 0; i < concurrency; i++ {
		go func(i int) {
			timeOffset := time.Duration(timeOffsetUnit * int64(i))
			results[i] <- workload(i, NewRateLimiter(maximumRate, timeOffset))
			close(results[i])
		}(i)
	}

	var result MergedResult
	result.Latency = NewHistogram()
	for _, ch := range results {
		res := <-ch
		result.Time += res.ElapsedTime
		result.Operations += res.Operations
		result.ClusteringRows += res.ClusteringRows
		result.OperationsPerSecond += float64(res.Operations) / res.ElapsedTime.Seconds()
		result.ClusteringRowsPerSecond += float64(res.ClusteringRows) / res.ElapsedTime.Seconds()
		dropped := result.Latency.Merge(res.Latency)
		if dropped > 0 {
			log.Print("dropped: ", dropped)
		}

	}
	result.Time /= time.Duration(concurrency)
	return result
}

func DoWrites(session *gocql.Session, workload WorkloadGenerator, rateLimiter RateLimiter) Result {
	value := make([]byte, clusteringRowSize)
	query := session.Query("INSERT INTO " + keyspaceName + "." + tableName + " (pk, ck, v) VALUES (?, ?, ?)")

	var operations int
	latencyHistogram := NewHistogram()

	start := time.Now()
	for !workload.IsDone() && atomic.LoadUint32(&stopAll) == 0 {
		rateLimiter.Wait()

		operations++
		pk := workload.NextPartitionKey()
		ck := workload.NextClusteringKey()
		bound := query.Bind(pk, ck, value)

		requestStart := time.Now()
		err := bound.Exec()
		requestEnd := time.Now()
		if err != nil {
			HandleError(err)
			break
		}

		latency := requestEnd.Sub(requestStart)
		err = latencyHistogram.RecordCorrectedValue(latency.Nanoseconds(), rateLimiter.ExpectedInterval())
		if err != nil {
			HandleError(err)
			break
		}
	}
	end := time.Now()

	return Result{end.Sub(start), operations, operations, latencyHistogram}
}

func DoBatchedWrites(session *gocql.Session, workload WorkloadGenerator, rateLimiter RateLimiter) Result {
	value := make([]byte, clusteringRowSize)
	request := fmt.Sprintf("INSERT INTO %s.%s (pk, ck, v) VALUES (?, ?, ?)", keyspaceName, tableName)

	var result Result
	result.Latency = NewHistogram()

	start := time.Now()
	for !workload.IsDone() && atomic.LoadUint32(&stopAll) == 0 {
		rateLimiter.Wait()

		batch := gocql.NewBatch(gocql.UnloggedBatch)
		batchSize := 0

		currentPk := workload.NextPartitionKey()
		for !workload.IsPartitionDone() && atomic.LoadUint32(&stopAll) == 0 && batchSize < rowsPerRequest {
			ck := workload.NextClusteringKey()
			batchSize++
			batch.Query(request, currentPk, ck, value)
		}

		result.Operations++
		result.ClusteringRows += batchSize

		requestStart := time.Now()
		err := session.ExecuteBatch(batch)
		requestEnd := time.Now()
		if err != nil {
			HandleError(err)
			break
		}

		latency := requestEnd.Sub(requestStart)
		err = result.Latency.RecordCorrectedValue(latency.Nanoseconds(), rateLimiter.ExpectedInterval())
		if err != nil {
			HandleError(err)
			break
		}
	}
	end := time.Now()

	result.ElapsedTime = end.Sub(start)
	return result
}

func DoCounterUpdates(session *gocql.Session, workload WorkloadGenerator, rateLimiter RateLimiter) Result {
	query := session.Query("UPDATE " + keyspaceName + "." + counterTableName + " SET c1 = c1 + 1, c2 = c2 + 1, c3 = c3 + 1, c4 = c4 + 1, c5 = c5 + 1 WHERE pk = ? AND ck = ?")

	var operations int
	latencyHistogram := NewHistogram()

	start := time.Now()
	for !workload.IsDone() && atomic.LoadUint32(&stopAll) == 0 {
		rateLimiter.Wait()

		operations++
		pk := workload.NextPartitionKey()
		ck := workload.NextClusteringKey()
		bound := query.Bind(pk, ck)

		requestStart := time.Now()
		err := bound.Exec()
		requestEnd := time.Now()
		if err != nil {
			HandleError(err)
			break
		}

		latency := requestEnd.Sub(requestStart)
		err = latencyHistogram.RecordCorrectedValue(latency.Nanoseconds(), rateLimiter.ExpectedInterval())
		if err != nil {
			HandleError(err)
			break
		}
	}
	end := time.Now()

	return Result{end.Sub(start), operations, operations, latencyHistogram}
}

func DoReads(session *gocql.Session, workload WorkloadGenerator, rateLimiter RateLimiter) Result {
	var request string
	if inRestriction {
		arr := make([]string, rowsPerRequest)
		for i := 0; i < rowsPerRequest; i++ {
			arr[i] = "?"
		}
		request = fmt.Sprintf("SELECT * from %s.%s WHERE pk = ? AND ck IN (%s)", keyspaceName, tableName, strings.Join(arr, ", "))
	} else if provideUpperBound {
		request = fmt.Sprintf("SELECT * FROM %s.%s WHERE pk = ? AND ck >= ? AND ck < ?", keyspaceName, tableName)
	} else {
		request = fmt.Sprintf("SELECT * FROM %s.%s WHERE pk = ? AND ck >= ? LIMIT %d", keyspaceName, tableName, rowsPerRequest)
	}
	query := session.Query(request)

	var result Result
	result.Latency = NewHistogram()

	start := time.Now()
	for !workload.IsDone() && atomic.LoadUint32(&stopAll) == 0 {
		rateLimiter.Wait()

		result.Operations++
		pk := workload.NextPartitionKey()

		var bound *gocql.Query
		if inRestriction {
			args := make([]interface{}, 1, rowsPerRequest+1)
			args[0] = pk
			for i := 0; i < rowsPerRequest; i++ {
				if workload.IsPartitionDone() {
					args = append(args, 0)
				} else {
					args = append(args, workload.NextClusteringKey())
				}
			}
			bound = query.Bind(args...)
		} else {
			ck := workload.NextClusteringKey()
			if provideUpperBound {
				bound = query.Bind(pk, ck, ck+rowsPerRequest)
			} else {
				bound = query.Bind(pk, ck)
			}
		}

		requestStart := time.Now()
		iter := bound.Iter()
		for iter.Scan(nil, nil, nil) {
			result.ClusteringRows++
		}
		requestEnd := time.Now()

		err := iter.Close()
		if err != nil {
			HandleError(err)
			break
		}

		latency := requestEnd.Sub(requestStart)
		err = result.Latency.RecordCorrectedValue(latency.Nanoseconds(), rateLimiter.ExpectedInterval())
		if err != nil {
			HandleError(err)
			break
		}
	}
	end := time.Now()

	result.ElapsedTime = end.Sub(start)
	return result
}
