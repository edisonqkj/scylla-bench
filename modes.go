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

type valueType10 struct {
	v01     []byte    `cql:"v01"`
	v02     []byte    `cql:"v02"`
	v03     []byte    `cql:"v03"`
	v04     []byte    `cql:"v04"`
	v05     []byte    `cql:"v05"`
	v06     []byte    `cql:"v06"`
	v07     []byte    `cql:"v07"`
	v08     []byte    `cql:"v08"`
	v09     []byte    `cql:"v09"`
	v10     []byte    `cql:"v10"`
}

type valueType20 struct {
	v01     []byte    `cql:"v01"`
	v02     []byte    `cql:"v02"`
	v03     []byte    `cql:"v03"`
	v04     []byte    `cql:"v04"`
	v05     []byte    `cql:"v05"`
	v06     []byte    `cql:"v06"`
	v07     []byte    `cql:"v07"`
	v08     []byte    `cql:"v08"`
	v09     []byte    `cql:"v09"`
	v10     []byte    `cql:"v10"`
	v11     []byte    `cql:"v11"`
	v12     []byte    `cql:"v12"`
	v13     []byte    `cql:"v13"`
	v14     []byte    `cql:"v14"`
	v15     []byte    `cql:"v15"`
	v16     []byte    `cql:"v16"`
	v17     []byte    `cql:"v17"`
	v18     []byte    `cql:"v18"`
	v19     []byte    `cql:"v19"`
	v20     []byte    `cql:"v20"`
}

type valueType30 struct {
	v01     []byte    `cql:"v01"`
	v02     []byte    `cql:"v02"`
	v03     []byte    `cql:"v03"`
	v04     []byte    `cql:"v04"`
	v05     []byte    `cql:"v05"`
	v06     []byte    `cql:"v06"`
	v07     []byte    `cql:"v07"`
	v08     []byte    `cql:"v08"`
	v09     []byte    `cql:"v09"`
	v10     []byte    `cql:"v10"`
	v11     []byte    `cql:"v11"`
	v12     []byte    `cql:"v12"`
	v13     []byte    `cql:"v13"`
	v14     []byte    `cql:"v14"`
	v15     []byte    `cql:"v15"`
	v16     []byte    `cql:"v16"`
	v17     []byte    `cql:"v17"`
	v18     []byte    `cql:"v18"`
	v19     []byte    `cql:"v19"`
	v20     []byte    `cql:"v20"`
	v21     []byte    `cql:"v21"`
	v22     []byte    `cql:"v22"`
	v23     []byte    `cql:"v23"`
	v24     []byte    `cql:"v24"`
	v25     []byte    `cql:"v25"`
	v26     []byte    `cql:"v26"`
	v27     []byte    `cql:"v27"`
	v28     []byte    `cql:"v28"`
	v29     []byte    `cql:"v29"`
	v30     []byte    `cql:"v30"`
}

type valueType40 struct {
	v01     []byte    `cql:"v01"`
	v02     []byte    `cql:"v02"`
	v03     []byte    `cql:"v03"`
	v04     []byte    `cql:"v04"`
	v05     []byte    `cql:"v05"`
	v06     []byte    `cql:"v06"`
	v07     []byte    `cql:"v07"`
	v08     []byte    `cql:"v08"`
	v09     []byte    `cql:"v09"`
	v10     []byte    `cql:"v10"`
	v11     []byte    `cql:"v11"`
	v12     []byte    `cql:"v12"`
	v13     []byte    `cql:"v13"`
	v14     []byte    `cql:"v14"`
	v15     []byte    `cql:"v15"`
	v16     []byte    `cql:"v16"`
	v17     []byte    `cql:"v17"`
	v18     []byte    `cql:"v18"`
	v19     []byte    `cql:"v19"`
	v20     []byte    `cql:"v20"`
	v21     []byte    `cql:"v21"`
	v22     []byte    `cql:"v22"`
	v23     []byte    `cql:"v23"`
	v24     []byte    `cql:"v24"`
	v25     []byte    `cql:"v25"`
	v26     []byte    `cql:"v26"`
	v27     []byte    `cql:"v27"`
	v28     []byte    `cql:"v28"`
	v29     []byte    `cql:"v29"`
	v30     []byte    `cql:"v30"`
	v31     []byte    `cql:"v31"`
	v32     []byte    `cql:"v32"`
	v33     []byte    `cql:"v33"`
	v34     []byte    `cql:"v34"`
	v35     []byte    `cql:"v35"`
	v36     []byte    `cql:"v36"`
	v37     []byte    `cql:"v37"`
	v38     []byte    `cql:"v38"`
	v39     []byte    `cql:"v39"`
	v40     []byte    `cql:"v40"`
}

type valueType50 struct {
	v01     []byte    `cql:"v01"`
	v02     []byte    `cql:"v02"`
	v03     []byte    `cql:"v03"`
	v04     []byte    `cql:"v04"`
	v05     []byte    `cql:"v05"`
	v06     []byte    `cql:"v06"`
	v07     []byte    `cql:"v07"`
	v08     []byte    `cql:"v08"`
	v09     []byte    `cql:"v09"`
	v10     []byte    `cql:"v10"`
	v11     []byte    `cql:"v11"`
	v12     []byte    `cql:"v12"`
	v13     []byte    `cql:"v13"`
	v14     []byte    `cql:"v14"`
	v15     []byte    `cql:"v15"`
	v16     []byte    `cql:"v16"`
	v17     []byte    `cql:"v17"`
	v18     []byte    `cql:"v18"`
	v19     []byte    `cql:"v19"`
	v20     []byte    `cql:"v20"`
	v21     []byte    `cql:"v21"`
	v22     []byte    `cql:"v22"`
	v23     []byte    `cql:"v23"`
	v24     []byte    `cql:"v24"`
	v25     []byte    `cql:"v25"`
	v26     []byte    `cql:"v26"`
	v27     []byte    `cql:"v27"`
	v28     []byte    `cql:"v28"`
	v29     []byte    `cql:"v29"`
	v30     []byte    `cql:"v30"`
	v31     []byte    `cql:"v31"`
	v32     []byte    `cql:"v32"`
	v33     []byte    `cql:"v33"`
	v34     []byte    `cql:"v34"`
	v35     []byte    `cql:"v35"`
	v36     []byte    `cql:"v36"`
	v37     []byte    `cql:"v37"`
	v38     []byte    `cql:"v38"`
	v39     []byte    `cql:"v39"`
	v40     []byte    `cql:"v40"`
	v41     []byte    `cql:"v41"`
	v42     []byte    `cql:"v42"`
	v43     []byte    `cql:"v43"`
	v44     []byte    `cql:"v44"`
	v45     []byte    `cql:"v45"`
	v46     []byte    `cql:"v46"`
	v47     []byte    `cql:"v47"`
	v48     []byte    `cql:"v48"`
	v49     []byte    `cql:"v49"`
	v50     []byte    `cql:"v50"`
}

func (v valueType10) MarshalUDT(name string, info gocql.TypeInfo) ([]byte, error) {
	switch name {
	case "v01":
		return gocql.Marshal(info, v.v01)
	case "v02":
		return gocql.Marshal(info, v.v02)
	case "v03":
		return gocql.Marshal(info, v.v03)
	case "v04":
		return gocql.Marshal(info, v.v04)
	case "v05":
		return gocql.Marshal(info, v.v05)
	case "v06":
		return gocql.Marshal(info, v.v06)
	case "v07":
		return gocql.Marshal(info, v.v07)
	case "v08":
		return gocql.Marshal(info, v.v08)
	case "v09":
		return gocql.Marshal(info, v.v09)
	case "v10":
		return gocql.Marshal(info, v.v10)
	default:
		return nil, fmt.Errorf("unknown column for valueType10: %q", name)
	}
}

func (v *valueType10) UnmarshalUDT(name string, info gocql.TypeInfo, data []byte) error {
	switch name {
	case "v01":
		return gocql.Unmarshal(info, data, &v.v01)
	case "v02":
		return gocql.Unmarshal(info, data, &v.v02)
	case "v03":
		return gocql.Unmarshal(info, data, &v.v03)
	case "v04":
		return gocql.Unmarshal(info, data, &v.v04)
	case "v05":
		return gocql.Unmarshal(info, data, &v.v05)
	case "v06":
		return gocql.Unmarshal(info, data, &v.v06)
	case "v07":
		return gocql.Unmarshal(info, data, &v.v07)
	case "v08":
		return gocql.Unmarshal(info, data, &v.v08)
	case "v09":
		return gocql.Unmarshal(info, data, &v.v09)
	case "v10":
		return gocql.Unmarshal(info, data, &v.v10)
	default:
		return fmt.Errorf("unknown column for valueType10: %q", name)
	}
}

func (v valueType20) MarshalUDT(name string, info gocql.TypeInfo) ([]byte, error) {
	switch name {
	case "v01":
		return gocql.Marshal(info, v.v01)
	case "v02":
		return gocql.Marshal(info, v.v02)
	case "v03":
		return gocql.Marshal(info, v.v03)
	case "v04":
		return gocql.Marshal(info, v.v04)
	case "v05":
		return gocql.Marshal(info, v.v05)
	case "v06":
		return gocql.Marshal(info, v.v06)
	case "v07":
		return gocql.Marshal(info, v.v07)
	case "v08":
		return gocql.Marshal(info, v.v08)
	case "v09":
		return gocql.Marshal(info, v.v09)
	case "v10":
		return gocql.Marshal(info, v.v10)
	case "v11":
		return gocql.Marshal(info, v.v11)
	case "v12":
		return gocql.Marshal(info, v.v12)
	case "v13":
		return gocql.Marshal(info, v.v13)
	case "v14":
		return gocql.Marshal(info, v.v14)
	case "v15":
		return gocql.Marshal(info, v.v15)
	case "v16":
		return gocql.Marshal(info, v.v16)
	case "v17":
		return gocql.Marshal(info, v.v17)
	case "v18":
		return gocql.Marshal(info, v.v18)
	case "v19":
		return gocql.Marshal(info, v.v19)
	case "v20":
		return gocql.Marshal(info, v.v20)
	default:
		return nil, fmt.Errorf("unknown column for valueType10: %q", name)
	}
}

func (v *valueType20) UnmarshalUDT(name string, info gocql.TypeInfo, data []byte) error {
	switch name {
	case "v01":
		return gocql.Unmarshal(info, data, &v.v01)
	case "v02":
		return gocql.Unmarshal(info, data, &v.v02)
	case "v03":
		return gocql.Unmarshal(info, data, &v.v03)
	case "v04":
		return gocql.Unmarshal(info, data, &v.v04)
	case "v05":
		return gocql.Unmarshal(info, data, &v.v05)
	case "v06":
		return gocql.Unmarshal(info, data, &v.v06)
	case "v07":
		return gocql.Unmarshal(info, data, &v.v07)
	case "v08":
		return gocql.Unmarshal(info, data, &v.v08)
	case "v09":
		return gocql.Unmarshal(info, data, &v.v09)
	case "v10":
		return gocql.Unmarshal(info, data, &v.v10)
	case "v11":
		return gocql.Unmarshal(info, data, &v.v11)
	case "v12":
		return gocql.Unmarshal(info, data, &v.v12)
	case "v13":
		return gocql.Unmarshal(info, data, &v.v13)
	case "v14":
		return gocql.Unmarshal(info, data, &v.v14)
	case "v15":
		return gocql.Unmarshal(info, data, &v.v15)
	case "v16":
		return gocql.Unmarshal(info, data, &v.v16)
	case "v17":
		return gocql.Unmarshal(info, data, &v.v17)
	case "v18":
		return gocql.Unmarshal(info, data, &v.v18)
	case "v19":
		return gocql.Unmarshal(info, data, &v.v19)
	case "v20":
		return gocql.Unmarshal(info, data, &v.v20)
	default:
		return fmt.Errorf("unknown column for valueType10: %q", name)
	}
}


func (v valueType30) MarshalUDT(name string, info gocql.TypeInfo) ([]byte, error) {
	switch name {
	case "v01":
		return gocql.Marshal(info, v.v01)
	case "v02":
		return gocql.Marshal(info, v.v02)
	case "v03":
		return gocql.Marshal(info, v.v03)
	case "v04":
		return gocql.Marshal(info, v.v04)
	case "v05":
		return gocql.Marshal(info, v.v05)
	case "v06":
		return gocql.Marshal(info, v.v06)
	case "v07":
		return gocql.Marshal(info, v.v07)
	case "v08":
		return gocql.Marshal(info, v.v08)
	case "v09":
		return gocql.Marshal(info, v.v09)
	case "v10":
		return gocql.Marshal(info, v.v10)
	case "v11":
		return gocql.Marshal(info, v.v11)
	case "v12":
		return gocql.Marshal(info, v.v12)
	case "v13":
		return gocql.Marshal(info, v.v13)
	case "v14":
		return gocql.Marshal(info, v.v14)
	case "v15":
		return gocql.Marshal(info, v.v15)
	case "v16":
		return gocql.Marshal(info, v.v16)
	case "v17":
		return gocql.Marshal(info, v.v17)
	case "v18":
		return gocql.Marshal(info, v.v18)
	case "v19":
		return gocql.Marshal(info, v.v19)
	case "v20":
		return gocql.Marshal(info, v.v20)
	case "v21":
		return gocql.Marshal(info, v.v21)
	case "v22":
		return gocql.Marshal(info, v.v22)
	case "v23":
		return gocql.Marshal(info, v.v23)
	case "v24":
		return gocql.Marshal(info, v.v24)
	case "v25":
		return gocql.Marshal(info, v.v25)
	case "v26":
		return gocql.Marshal(info, v.v26)
	case "v27":
		return gocql.Marshal(info, v.v27)
	case "v28":
		return gocql.Marshal(info, v.v28)
	case "v29":
		return gocql.Marshal(info, v.v29)
	case "v30":
		return gocql.Marshal(info, v.v30)
	default:
		return nil, fmt.Errorf("unknown column for valueType10: %q", name)
	}
}

func (v *valueType30) UnmarshalUDT(name string, info gocql.TypeInfo, data []byte) error {
	switch name {
	case "v01":
		return gocql.Unmarshal(info, data, &v.v01)
	case "v02":
		return gocql.Unmarshal(info, data, &v.v02)
	case "v03":
		return gocql.Unmarshal(info, data, &v.v03)
	case "v04":
		return gocql.Unmarshal(info, data, &v.v04)
	case "v05":
		return gocql.Unmarshal(info, data, &v.v05)
	case "v06":
		return gocql.Unmarshal(info, data, &v.v06)
	case "v07":
		return gocql.Unmarshal(info, data, &v.v07)
	case "v08":
		return gocql.Unmarshal(info, data, &v.v08)
	case "v09":
		return gocql.Unmarshal(info, data, &v.v09)
	case "v10":
		return gocql.Unmarshal(info, data, &v.v10)
	case "v11":
		return gocql.Unmarshal(info, data, &v.v11)
	case "v12":
		return gocql.Unmarshal(info, data, &v.v12)
	case "v13":
		return gocql.Unmarshal(info, data, &v.v13)
	case "v14":
		return gocql.Unmarshal(info, data, &v.v14)
	case "v15":
		return gocql.Unmarshal(info, data, &v.v15)
	case "v16":
		return gocql.Unmarshal(info, data, &v.v16)
	case "v17":
		return gocql.Unmarshal(info, data, &v.v17)
	case "v18":
		return gocql.Unmarshal(info, data, &v.v18)
	case "v19":
		return gocql.Unmarshal(info, data, &v.v19)
	case "v20":
		return gocql.Unmarshal(info, data, &v.v20)
	case "v21":
		return gocql.Unmarshal(info, data, &v.v21)
	case "v22":
		return gocql.Unmarshal(info, data, &v.v22)
	case "v23":
		return gocql.Unmarshal(info, data, &v.v23)
	case "v24":
		return gocql.Unmarshal(info, data, &v.v24)
	case "v25":
		return gocql.Unmarshal(info, data, &v.v25)
	case "v26":
		return gocql.Unmarshal(info, data, &v.v26)
	case "v27":
		return gocql.Unmarshal(info, data, &v.v27)
	case "v28":
		return gocql.Unmarshal(info, data, &v.v28)
	case "v29":
		return gocql.Unmarshal(info, data, &v.v29)
	case "v30":
		return gocql.Unmarshal(info, data, &v.v30)
	default:
		return fmt.Errorf("unknown column for valueType10: %q", name)
	}
}

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
	Period              time.Duration
	StartTime           time.Time
	CompletedOperations int64
}

func (mxrl *MaximumRateLimiter) Wait() {
	mxrl.CompletedOperations++
	nextRequest := mxrl.StartTime.Add(mxrl.Period * time.Duration(mxrl.CompletedOperations))
	now := time.Now()
	if now.Before(nextRequest) {
		time.Sleep(nextRequest.Sub(now))
	}
}

func (mxrl *MaximumRateLimiter) ExpectedInterval() int64 {
	return mxrl.Period.Nanoseconds()
}

func NewRateLimiter(maximumRate int, timeOffset time.Duration) RateLimiter {
	if maximumRate == 0 {
		return &UnlimitedRateLimiter{}
	}
	period := time.Duration(int64(time.Second) / int64(maximumRate))
	return &MaximumRateLimiter{period, time.Now(), 0}
}

type Result struct {
	Final          bool
	ElapsedTime    time.Duration
	Operations     int
	ClusteringRows int
	Errors         int
	Latency        *hdrhistogram.Histogram
}

type MergedResult struct {
	Time                    time.Duration
	Operations              int
	ClusteringRows          int
	OperationsPerSecond     float64
	ClusteringRowsPerSecond float64
	Errors                  int
	Latency                 *hdrhistogram.Histogram
}

func NewMergedResult() *MergedResult {
	result := &MergedResult{}
	result.Latency = NewHistogram()
	return result
}

func (mr *MergedResult) AddResult(result Result) {
	mr.Time += result.ElapsedTime
	mr.Operations += result.Operations
	mr.ClusteringRows += result.ClusteringRows
	mr.OperationsPerSecond += float64(result.Operations) / result.ElapsedTime.Seconds()
	mr.ClusteringRowsPerSecond += float64(result.ClusteringRows) / result.ElapsedTime.Seconds()
	mr.Errors += result.Errors
	if measureLatency {
		dropped := mr.Latency.Merge(result.Latency)
		if dropped > 0 {
			log.Print("dropped: ", dropped)
		}
	}
}

func NewHistogram() *hdrhistogram.Histogram {
	if !measureLatency {
		return nil
	}
	return hdrhistogram.New(time.Microsecond.Nanoseconds()*50, (timeout + timeout*2).Nanoseconds(), 3)
}

func HandleError(err error) {
	if atomic.SwapUint32(&stopAll, 1) == 0 {
		log.Print(err)
		fmt.Println("\nstopping")
		atomic.StoreUint32(&stopAll, 1)
	}
}

func MergeResults(results []chan Result) (bool, *MergedResult) {
	result := NewMergedResult()
	final := false
	for i, ch := range results {
		res := <-ch
		if !final && res.Final {
			final = true
			result = NewMergedResult()
			for _, ch2 := range results[0:i] {
				res = <-ch2
				for !res.Final {
					res = <-ch2
				}
				result.AddResult(res)
			}
		} else if final && !res.Final {
			for !res.Final {
				res = <-ch
			}
		}
		result.AddResult(res)
	}
	result.Time /= time.Duration(concurrency)
	return final, result
}

func RunConcurrently(maximumRate int, workload func(id int, resultChannel chan Result, rateLimiter RateLimiter)) *MergedResult {
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

	startTime := time.Now()
	for i := 0; i < concurrency; i++ {
		go func(i int) {
			timeOffset := time.Duration(timeOffsetUnit * int64(i))
			workload(i, results[i], NewRateLimiter(maximumRate, timeOffset))
			close(results[i])
		}(i)
	}

	final, result := MergeResults(results)
	for !final {
		result.Time = time.Now().Sub(startTime)
		PrintPartialResult(result)
		final, result = MergeResults(results)
	}
	return result
}

type ResultBuilder struct {
	FullResult    *Result
	PartialResult *Result
}

func NewResultBuilder() *ResultBuilder {
	rb := &ResultBuilder{}
	rb.FullResult = &Result{}
	rb.PartialResult = &Result{}
	rb.FullResult.Final = true
	rb.FullResult.Latency = NewHistogram()
	rb.PartialResult.Latency = NewHistogram()
	return rb
}

func (rb *ResultBuilder) IncOps() {
	rb.FullResult.Operations++
	rb.PartialResult.Operations++
}

func (rb *ResultBuilder) IncRows() {
	rb.FullResult.ClusteringRows++
	rb.PartialResult.ClusteringRows++
}

func (rb *ResultBuilder) AddRows(n int) {
	rb.FullResult.ClusteringRows += n
	rb.PartialResult.ClusteringRows += n
}

func (rb *ResultBuilder) IncErrors() {
	rb.FullResult.Errors++
	rb.PartialResult.Errors++
}

func (rb *ResultBuilder) ResetPartialResult() {
	rb.PartialResult = &Result{}
	rb.PartialResult.Latency = NewHistogram()
}

func (rb *ResultBuilder) RecordLatency(latency time.Duration, rateLimiter RateLimiter) error {
	if !measureLatency {
		return nil
	}

	err := rb.FullResult.Latency.RecordCorrectedValue(latency.Nanoseconds(), rateLimiter.ExpectedInterval())
	if err != nil {
		return err
	}

	err = rb.PartialResult.Latency.RecordCorrectedValue(latency.Nanoseconds(), rateLimiter.ExpectedInterval())
	if err != nil {
		return err
	}

	return nil
}

var errorRecordingLatency bool

func RunTest(resultChannel chan Result, workload WorkloadGenerator, rateLimiter RateLimiter, test func(rb *ResultBuilder) (error, time.Duration)) {
	rb := NewResultBuilder()

	start := time.Now()
	partialStart := start
	for !workload.IsDone() && atomic.LoadUint32(&stopAll) == 0 {
		rateLimiter.Wait()

		err, latency := test(rb)
		if err != nil {
			log.Print(err)
			rb.IncErrors()
			continue
		}

		err = rb.RecordLatency(latency, rateLimiter)
		if err != nil {
			errorRecordingLatency = true
		}

		now := time.Now()
		if now.Sub(partialStart) > time.Second {
			resultChannel <- *rb.PartialResult
			rb.ResetPartialResult()
			partialStart = now
		}
	}
	end := time.Now()

	rb.FullResult.ElapsedTime = end.Sub(start)
	resultChannel <- *rb.FullResult
}

func getWriteQuery() string{
   columnNames := "pk,ck"
   columnArgs := "?,?"
   for i:=0; i < clusteringColumnCount; i++ {
       columnNames = fmt.Sprintf("%s, v%02d",columnNames,i)
       columnArgs = fmt.Sprintf("%s, ?",columnArgs)
   }
   return fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s)", keyspaceName, tableName,columnNames,columnArgs)
}
func DoWrites(session *gocql.Session, resultChannel chan Result, workload WorkloadGenerator, rateLimiter RateLimiter) {
	value := make([]byte, clusteringRowSize)
        values := make([]interface{}, clusteringColumnCount+2);
        for i:=2; i < len(values); i++ {
            values[i] = value;
        }
// 	query := session.Query("INSERT INTO " + keyspaceName + "." + tableName + " (pk, ck, v) VALUES (?, ?, ?)")
        query := session.Query(getWriteQuery())

	RunTest(resultChannel, workload, rateLimiter, func(rb *ResultBuilder) (error, time.Duration) {
		pk := workload.NextPartitionKey()
		ck := workload.NextClusteringKey()
                values[0]=pk;
                values[1]=ck;
		//bound := query.Bind(pk, ck, value)
		bound := query.Bind(values...)

		requestStart := time.Now()
		err := bound.Exec()
		requestEnd := time.Now()
		if err != nil {
			return err, time.Duration(0)
		}

		rb.IncOps()
		rb.IncRows()

		latency := requestEnd.Sub(requestStart)
		return nil, latency
	})
}

func DoWritesUDT(session *gocql.Session, resultChannel chan Result, workload WorkloadGenerator, rateLimiter RateLimiter) {
	value := make([]byte, clusteringRowSize)
	query := session.Query("INSERT INTO " + keyspaceName + "." + tableName + " (pk, ck, v) VALUES (?, ?, ?)")
        var valueType interface{}
        if (clusteringColumnCount == 10) {
           valueType = &valueType10{value,value,value,value,value,value,value,value,value,value}
        }
        if (clusteringColumnCount == 20) {
           valueType = &valueType20{value,value,value,value,value,value,value,value,value,value,
                                    value,value,value,value,value,value,value,value,value,value}
        }
        if (clusteringColumnCount == 30) {
           valueType = &valueType30{value,value,value,value,value,value,value,value,value,value,
                                    value,value,value,value,value,value,value,value,value,value,
                                    value,value,value,value,value,value,value,value,value,value}
        }

	RunTest(resultChannel, workload, rateLimiter, func(rb *ResultBuilder) (error, time.Duration) {
		pk := workload.NextPartitionKey()
		ck := workload.NextClusteringKey()
		bound := query.Bind(pk, ck, valueType);

		requestStart := time.Now()
		err := bound.Exec()
		requestEnd := time.Now()
		if err != nil {
			return err, time.Duration(0)
		}

		rb.IncOps()
		rb.IncRows()

		latency := requestEnd.Sub(requestStart)
		return nil, latency
	})
}

func DoBatchedWrites(session *gocql.Session, resultChannel chan Result, workload WorkloadGenerator, rateLimiter RateLimiter) {
	value := make([]byte, clusteringRowSize)
	request := fmt.Sprintf("INSERT INTO %s.%s (pk, ck, v) VALUES (?, ?, ?)", keyspaceName, tableName)

	RunTest(resultChannel, workload, rateLimiter, func(rb *ResultBuilder) (error, time.Duration) {
		batch := gocql.NewBatch(gocql.UnloggedBatch)
		batchSize := 0

		currentPk := workload.NextPartitionKey()
		for !workload.IsPartitionDone() && atomic.LoadUint32(&stopAll) == 0 && batchSize < rowsPerRequest {
			ck := workload.NextClusteringKey()
			batchSize++
			batch.Query(request, currentPk, ck, value)
		}

		requestStart := time.Now()
		err := session.ExecuteBatch(batch)
		requestEnd := time.Now()
		if err != nil {
			return err, time.Duration(0)
		}

		rb.IncOps()
		rb.AddRows(batchSize)

		latency := requestEnd.Sub(requestStart)
		return nil, latency
	})
}

func DoCounterUpdates(session *gocql.Session, resultChannel chan Result, workload WorkloadGenerator, rateLimiter RateLimiter) {
	query := session.Query("UPDATE " + keyspaceName + "." + counterTableName + " SET c1 = c1 + 1, c2 = c2 + 1, c3 = c3 + 1, c4 = c4 + 1, c5 = c5 + 1 WHERE pk = ? AND ck = ?")

	RunTest(resultChannel, workload, rateLimiter, func(rb *ResultBuilder) (error, time.Duration) {
		pk := workload.NextPartitionKey()
		ck := workload.NextClusteringKey()
		bound := query.Bind(pk, ck)

		requestStart := time.Now()
		err := bound.Exec()
		requestEnd := time.Now()
		if err != nil {
			return err, time.Duration(0)
		}

		rb.IncOps()
		rb.IncRows()

		latency := requestEnd.Sub(requestStart)
		return nil, latency
	})
}

func DoReads(session *gocql.Session, resultChannel chan Result, workload WorkloadGenerator, rateLimiter RateLimiter) {
	DoReadsFromTable(tableName, session, resultChannel, workload, rateLimiter)
}

func DoCounterReads(session *gocql.Session, resultChannel chan Result, workload WorkloadGenerator, rateLimiter RateLimiter) {
	DoReadsFromTable(counterTableName, session, resultChannel, workload, rateLimiter)
}

func DoReadsFromTable(table string, session *gocql.Session, resultChannel chan Result, workload WorkloadGenerator, rateLimiter RateLimiter) {
	var request string
	if inRestriction {
		arr := make([]string, rowsPerRequest)
		for i := 0; i < rowsPerRequest; i++ {
			arr[i] = "?"
		}
		request = fmt.Sprintf("SELECT * from %s.%s WHERE pk = ? AND ck IN (%s)", keyspaceName, table, strings.Join(arr, ", "))
	} else if provideUpperBound {
		request = fmt.Sprintf("SELECT * FROM %s.%s WHERE pk = ? AND ck >= ? AND ck < ?", keyspaceName, table)
	} else if noLowerBound {
		//request = fmt.Sprintf("SELECT * FROM %s.%s WHERE pk = ? LIMIT %d", keyspaceName, table, rowsPerRequest)
		request = fmt.Sprintf("SELECT * FROM %s.%s WHERE pk = ?", keyspaceName, table)
                if !noCqlLimit {
                   request = fmt.Sprintf("%s LIMIT %d",request,rowsPerRequest)
                }
	} else {
		//request = fmt.Sprintf("SELECT * FROM %s.%s WHERE pk = ? AND ck >= ? LIMIT %d", keyspaceName, table, rowsPerRequest)
		request = fmt.Sprintf("SELECT * FROM %s.%s WHERE pk = ? AND ck >= ?", keyspaceName, table)
                if !noCqlLimit {
                   request = fmt.Sprintf("%s LIMIT %d",request,rowsPerRequest)
                }
	}
	query := session.Query(request)
        keyspaceMetadata,nil := session.KeyspaceMetadata(keyspaceName)
        tableMetadata := keyspaceMetadata.Tables[table]
        scanDestinationsCount := len(tableMetadata.Columns)// +  len(tableMetadata.PartitionKey)  + len(tableMetadata.ClusteringColumns)
        scanDestinations := make([]interface{}, scanDestinationsCount)

	RunTest(resultChannel, workload, rateLimiter, func(rb *ResultBuilder) (error, time.Duration) {
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
		} else if noLowerBound {
			bound = query.Bind(pk)
		} else {
			ck := workload.NextClusteringKey()
			if provideUpperBound {
				bound = query.Bind(pk, ck, ck+int64(rowsPerRequest))
			} else {
				bound = query.Bind(pk, ck)
			}
		}

		requestStart := time.Now()
		iter := bound.Iter()
		if table == tableName {
			for iter.Scan(scanDestinations...) {
				rb.IncRows()
			}
		} else {
			for iter.Scan(nil, nil, nil, nil, nil, nil, nil) {
				rb.IncRows()
			}
		}
		requestEnd := time.Now()

		err := iter.Close()
		if err != nil {
			return err, time.Duration(0)
		}

		rb.IncOps()

		latency := requestEnd.Sub(requestStart)
		return nil, latency
	})
}
