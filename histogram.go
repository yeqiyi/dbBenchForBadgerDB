package main

import (
	"fmt"
	"math"
)

const kNumBucket int = 154

var kBucketLimit [kNumBucket]float64 = [kNumBucket]float64{
	1,
	2,
	3,
	4,
	5,
	6,
	7,
	8,
	9,
	10,
	12,
	14,
	16,
	18,
	20,
	25,
	30,
	35,
	40,
	45,
	50,
	60,
	70,
	80,
	90,
	100,
	120,
	140,
	160,
	180,
	200,
	250,
	300,
	350,
	400,
	450,
	500,
	600,
	700,
	800,
	900,
	1000,
	1200,
	1400,
	1600,
	1800,
	2000,
	2500,
	3000,
	3500,
	4000,
	4500,
	5000,
	6000,
	7000,
	8000,
	9000,
	10000,
	12000,
	14000,
	16000,
	18000,
	20000,
	25000,
	30000,
	35000,
	40000,
	45000,
	50000,
	60000,
	70000,
	80000,
	90000,
	100000,
	120000,
	140000,
	160000,
	180000,
	200000,
	250000,
	300000,
	350000,
	400000,
	450000,
	500000,
	600000,
	700000,
	800000,
	900000,
	1000000,
	1200000,
	1400000,
	1600000,
	1800000,
	2000000,
	2500000,
	3000000,
	3500000,
	4000000,
	4500000,
	5000000,
	6000000,
	7000000,
	8000000,
	9000000,
	10000000,
	12000000,
	14000000,
	16000000,
	18000000,
	20000000,
	25000000,
	30000000,
	35000000,
	40000000,
	45000000,
	50000000,
	60000000,
	70000000,
	80000000,
	90000000,
	100000000,
	120000000,
	140000000,
	160000000,
	180000000,
	200000000,
	250000000,
	300000000,
	350000000,
	400000000,
	450000000,
	500000000,
	600000000,
	700000000,
	800000000,
	900000000,
	1000000000,
	1200000000,
	1400000000,
	1600000000,
	1800000000,
	2000000000,
	2500000000.0,
	3000000000.0,
	3500000000.0,
	4000000000.0,
	4500000000.0,
	5000000000.0,
	6000000000.0,
	7000000000.0,
	8000000000.0,
	9000000000.0,
	1e200,
}

type Histrogram struct {
	min       float64
	max       float64
	num       float64
	sum       float64
	sumSquare float64
	buckets   [kNumBucket]float64
}

func (h *Histrogram) Add(value float64) {
	// Linear search is fast enough
	b := 0
	for ; b < kNumBucket-1 && kBucketLimit[b] <= value; b++ {
	}
	h.buckets[b] += 1.0
	if h.min > value {
		h.min = value
	}
	if h.max < value {
		h.max = value
	}

	h.num++
	h.sum += value
	h.sumSquare += value * value
}

func (h *Histrogram) Median() float64 {
	return h.Percentile(50.)
}

func (h *Histrogram) Percentile(p float64) float64 {
	threshold := h.num * (p / 100.)
	sum := 0.
	for b := 0; b < kNumBucket; b++ {
		sum += h.buckets[b]
		if sum >= threshold {
			var leftPoint, rightPoint float64
			if b == 0 {
				leftPoint = 0
			} else {
				leftPoint = kBucketLimit[b-1]
			}

			rightPoint = kBucketLimit[b]
			leftSum := sum - h.buckets[b]
			rightSum := sum
			pos := (threshold - leftSum) / (rightSum - leftSum)
			r := leftPoint + (rightPoint-leftPoint)*pos
			if r < h.min {
				r = h.min
			}
			if r > h.max {
				r = h.max
			}
			return r
		}
	}
	return h.max
}

func (h *Histrogram) Average() float64 {
	if h.num == 0 {
		return 0.
	}
	return h.sum / h.num
}

func (h *Histrogram) Std() float64 {
	if h.num == 0 {
		return 0.
	}
	variance := (h.sumSquare*h.num - h.sum*h.sum) / (h.num * h.num)
	return math.Sqrt(variance)
}

func (h *Histrogram) Clear() {
	h.min = kBucketLimit[kNumBucket-1]
	h.max = 0
	h.sum = 0
	h.sumSquare = 0
	for i := 0; i < kNumBucket; i++ {
		h.buckets[i] = 0
	}
}

func (h *Histrogram) Merge(other *Histrogram) {
	if other.max < h.min {
		h.min = other.min
	}
	if other.min > h.max {
		h.max = other.max
	}
	h.num += other.num
	h.sum += other.sum
	h.sumSquare += other.sumSquare
	for b := 0; b < kNumBucket; b++ {
		h.buckets[b] += other.buckets[b]
	}
}

func (h *Histrogram) ToString() string {
	result := ""
	result += fmt.Sprintf("Count: %.0f Avg: %.4f Std: %.2f\n", h.num,
		h.Average(), h.Std())

	minVal := 0.
	if h.num == 0 {
		minVal = 0.
	} else {
		minVal = h.min
	}
	result += fmt.Sprintf("Min: %.4f Max: %.4f 50: %.4f 90: %.4f 99: %.4f 99.9: %.4f\n",
		minVal, h.max, h.Median(), h.Percentile(90), h.Percentile(99), h.Percentile((99.9)))
	return result
}
