package core

import (
	"fmt"
	"ignis/executor/core/ierror"
	"math"
	"regexp"
	"strconv"
	"strings"
)

type IPropertyParser struct {
	properties map[string]string
}

func NewIPropertyParser(properties map[string]string) *IPropertyParser {
	return &IPropertyParser{
		properties,
	}
}

func (this *IPropertyParser) Cores() (int64, error) { return this.GetNumber("ignis.executor.cores") }

func (this *IPropertyParser) TransportCores() (float64, error) {
	return this.GetMinFloat("ignis.transport.cores", 0)
}

func (this *IPropertyParser) PartitionMinimal() (int64, error) {
	return this.GetSize("ignis.partition.minimal")
}

func (this *IPropertyParser) SortSamples() (float64, error) {
	return this.GetMinFloat("ignis.modules.sort.samples", 0)
}

func (this *IPropertyParser) SortResampling() (bool, error) {
	return this.GetBool("ignis.modules.sort.resampling")
}

func (this *IPropertyParser) IoOverwrite() (bool, error) {
	return this.GetBool("ignis.modules.io.overwrite")
}

func (this *IPropertyParser) IoCores() (float64, error) {
	return this.GetMinFloat("ignis.modules.io.cores", 0)
}

func (this *IPropertyParser) IoCompression() (int64, error) {
	return this.GetRangeNumber("ignis.modules.io.compression", 0, 9)
}

func (this *IPropertyParser) MsgCompression() (int8, error) {
	vaue, err := this.GetRangeNumber("ignis.transport.compression", 0, 9)
	if err != nil {
		return 0, ierror.Raise(err)
	}
	return int8(vaue), nil
}

func (this *IPropertyParser) NativeSerialization() (bool, error) {
	value, err := this.GetString("ignis.partition.serialization")
	if err != nil {
		return false, err
	}
	return value == "native", nil
}

func (this *IPropertyParser) PartitionCompression() (int8, error) {
	n, err := this.GetRangeNumber("ignis.partition.compression", 0, 9)
	return int8(n), err
}

func (this *IPropertyParser) TransportElemSize() (int64, error) {
	return this.GetSize("ignis.transport.element.size")
}

func (this *IPropertyParser) PartitionType() (string, error) {
	return this.GetString("ignis.partition.type")
}

func (this *IPropertyParser) ExchangeType() (string, error) {
	return this.GetString("ignis.modules.exchange.type")
}

func (this *IPropertyParser) JobDirectory() (string, error) {
	return this.GetString("ignis.job.directory")
}

func (this *IPropertyParser) ExecutorDirectory() (string, error) {
	return this.GetString("ignis.executor.directory")
}

func (this *IPropertyParser) Host() (string, error) {
	return this.GetString("ignis.executor.host")
}

func (this *IPropertyParser) GetHdfsPath() (string, error) {
	return this.GetString("ignis.hdfs.namenode")
}

func (this *IPropertyParser) PreserveOrder() (bool, error) {
	return this.GetBool("ignis.hdfs.preserve.order")
}

func (this *IPropertyParser) GetString(key string) (string, error) {
	value, ok := this.properties[key]
	if ok {
		return value, nil
	}
	return "", ierror.RaiseMsg(key + " is empty")
}

func (this *IPropertyParser) GetNumber(key string) (int64, error) {
	value, err := this.GetString(key)
	if err != nil {
		return 0, err
	}
	num, err := strconv.ParseInt(value, 10, 0)
	if err != nil {
		return 0, ierror.Raise(err)
	}
	return num, nil
}

func (this *IPropertyParser) GetFloat(key string) (float64, error) {
	value, err := this.GetString(key)
	if err != nil {
		return 0, err
	}
	num, err := strconv.ParseFloat(value, 0)
	if err != nil {
		return 0, ierror.Raise(err)
	}
	return num, nil
}

func (this *IPropertyParser) GetMinNumber(key string, min int64) (int64, error) {
	value, err := this.GetNumber(key)
	if err != nil {
		return 0, err
	}
	if value < min {
		return 0, ierror.RaiseMsg(fmt.Sprintf("%s error %d is less than %d", key, value, min))
	}
	return value, nil
}

func (this *IPropertyParser) GetMaxNumber(key string, max int64) (int64, error) {
	value, err := this.GetNumber(key)
	if err != nil {
		return 0, err
	}
	if value > max {
		return 0, ierror.RaiseMsg(fmt.Sprintf("%s error %d is greater than %d", key, value, max))
	}
	return value, nil
}

func (this *IPropertyParser) GetRangeNumber(key string, min int64, max int64) (int64, error) {
	value, err := this.GetMinNumber(key, min)
	if err != nil {
		return 0, err
	}
	if value > max {
		return 0, ierror.RaiseMsg(fmt.Sprintf("%s error %d is greater than %d", key, value, max))
	}
	return value, nil
}

func (this *IPropertyParser) GetMinFloat(key string, min float64) (float64, error) {
	value, err := this.GetFloat(key)
	if err != nil {
		return 0, err
	}
	if value < min {
		return 0, ierror.RaiseMsg(fmt.Sprintf("%s error %f is less than %f", key, value, min))
	}
	return value, nil
}

func (this *IPropertyParser) GetMaxFloat(key string, max float64) (float64, error) {
	value, err := this.GetFloat(key)
	if err != nil {
		return 0, err
	}
	if value > max {
		return 0, ierror.RaiseMsg(fmt.Sprintf("%s error %f is greater than %f", key, value, max))
	}
	return value, nil
}

func (this *IPropertyParser) GetRangeFloat(key string, min float64, max float64) (float64, error) {
	value, err := this.GetMinFloat(key, min)
	if err != nil {
		return 0, err
	}
	if value > max {
		return 0, ierror.RaiseMsg(fmt.Sprintf("%s error %f is greater than %f", key, value, min))
	}
	return value, nil
}

func (this *IPropertyParser) GetSize(key string) (int64, error) {
	value, err := this.GetString(key)
	parseError := func(pos int) (int64, error) {
		return 0, ierror.RaiseMsg(fmt.Sprintf("%s parsing error %c (%d) in %s", key, value[pos], pos+1, value))
	}
	if err != nil {
		return 0, err
	}
	UNITS := "KMGTPEZY"
	decimal := false
	length := len(value)
	exp := 0
	i := 0
	var base int64
	for i < length {
		if '9' >= value[i] && value[i] >= '0' {
			i++
		} else if !decimal && (value[i] == '.' || value[i] == ',') {
			i++
			decimal = true
		} else {
			break
		}
	}
	num, err := strconv.ParseFloat(value[:i], 0)
	if err != nil {
		return 0, ierror.Raise(err)
	}
	if i < length {
		if value[i] == ' ' {
			i++
		}
	}
	if i < length {
		exp = strings.IndexByte(UNITS, strings.ToUpper(value)[i]) + 1
		if exp > 0 {
			i++
		}
	}
	if i < length && exp > 0 && value[i] == 'i' {
		i++
		base = 1024
	} else {
		base = 1000
	}
	if i < length {
		if value[i] == 'B' {

		} else if value[i] == 'b' {
			num = num / 8
		} else {
			return parseError(i)
		}
		i++
	}
	if i != length {
		return parseError(i)
	}
	return int64(math.Ceil(num * math.Pow(float64(base), float64(exp)))), nil
}

var bool_value, _ = regexp.Compile("y|Y|yes|Yes|YES|true|True|TRUE|on|On|ON")

func (this *IPropertyParser) GetBool(key string) (bool, error) {
	value, err := this.GetString(key)
	if err != nil {
		return false, err
	}
	return bool_value.MatchString(value), nil
}
