// Code generated by Thrift Compiler (0.14.1). DO NOT EDIT.

package main

import (
	"context"
	"flag"
	"fmt"
	"math"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"github.com/apache/thrift/lib/go/thrift"
	"ignis/rpc"
	"ignis/rpc/executor"
)

var _ = rpc.GoUnusedProtection__
var _ = executor.GoUnusedProtection__

func Usage() {
  fmt.Fprintln(os.Stderr, "Usage of ", os.Args[0], " [-h host:port] [-u url] [-f[ramed]] function [arg1 [arg2...]]:")
  flag.PrintDefaults()
  fmt.Fprintln(os.Stderr, "\nFunctions:")
  fmt.Fprintln(os.Stderr, "  void executeTo(ISource src)")
  fmt.Fprintln(os.Stderr, "  void map_(ISource src)")
  fmt.Fprintln(os.Stderr, "  void filter(ISource src)")
  fmt.Fprintln(os.Stderr, "  void flatmap(ISource src)")
  fmt.Fprintln(os.Stderr, "  void keyBy(ISource src)")
  fmt.Fprintln(os.Stderr, "  void mapPartitions(ISource src)")
  fmt.Fprintln(os.Stderr, "  void mapPartitionsWithIndex(ISource src, bool preservesPartitioning)")
  fmt.Fprintln(os.Stderr, "  void mapExecutor(ISource src)")
  fmt.Fprintln(os.Stderr, "  void mapExecutorTo(ISource src)")
  fmt.Fprintln(os.Stderr, "  void groupBy(ISource src, i64 numPartitions)")
  fmt.Fprintln(os.Stderr, "  void sort(bool ascending)")
  fmt.Fprintln(os.Stderr, "  void sort2(bool ascending, i64 numPartitions)")
  fmt.Fprintln(os.Stderr, "  void sortBy(ISource src, bool ascending)")
  fmt.Fprintln(os.Stderr, "  void sortBy3(ISource src, bool ascending, i64 numPartitions)")
  fmt.Fprintln(os.Stderr, "  void flatMapValues(ISource src)")
  fmt.Fprintln(os.Stderr, "  void mapValues(ISource src)")
  fmt.Fprintln(os.Stderr, "  void groupByKey(i64 numPartitions)")
  fmt.Fprintln(os.Stderr, "  void groupByKey2(i64 numPartitions, ISource src)")
  fmt.Fprintln(os.Stderr, "  void reduceByKey(ISource src, i64 numPartitions, bool localReduce)")
  fmt.Fprintln(os.Stderr, "  void aggregateByKey(ISource zero, ISource seqOp, i64 numPartitions)")
  fmt.Fprintln(os.Stderr, "  void aggregateByKey4(ISource zero, ISource seqOp, ISource combOp, i64 numPartitions)")
  fmt.Fprintln(os.Stderr, "  void foldByKey(ISource zero, ISource src, i64 numPartitions, bool localFold)")
  fmt.Fprintln(os.Stderr, "  void sortByKey(bool ascending)")
  fmt.Fprintln(os.Stderr, "  void sortByKey2a(bool ascending, i64 numPartitions)")
  fmt.Fprintln(os.Stderr, "  void sortByKey2b(ISource src, bool ascending)")
  fmt.Fprintln(os.Stderr, "  void sortByKey3(ISource src, bool ascending, i64 numPartitions)")
  fmt.Fprintln(os.Stderr)
  os.Exit(0)
}

type httpHeaders map[string]string

func (h httpHeaders) String() string {
  var m map[string]string = h
  return fmt.Sprintf("%s", m)
}

func (h httpHeaders) Set(value string) error {
  parts := strings.Split(value, ": ")
  if len(parts) != 2 {
    return fmt.Errorf("header should be of format 'Key: Value'")
  }
  h[parts[0]] = parts[1]
  return nil
}

func main() {
  flag.Usage = Usage
  var host string
  var port int
  var protocol string
  var urlString string
  var framed bool
  var useHttp bool
  headers := make(httpHeaders)
  var parsedUrl *url.URL
  var trans thrift.TTransport
  _ = strconv.Atoi
  _ = math.Abs
  flag.Usage = Usage
  flag.StringVar(&host, "h", "localhost", "Specify host and port")
  flag.IntVar(&port, "p", 9090, "Specify port")
  flag.StringVar(&protocol, "P", "binary", "Specify the protocol (binary, compact, simplejson, json)")
  flag.StringVar(&urlString, "u", "", "Specify the url")
  flag.BoolVar(&framed, "framed", false, "Use framed transport")
  flag.BoolVar(&useHttp, "http", false, "Use http")
  flag.Var(headers, "H", "Headers to set on the http(s) request (e.g. -H \"Key: Value\")")
  flag.Parse()
  
  if len(urlString) > 0 {
    var err error
    parsedUrl, err = url.Parse(urlString)
    if err != nil {
      fmt.Fprintln(os.Stderr, "Error parsing URL: ", err)
      flag.Usage()
    }
    host = parsedUrl.Host
    useHttp = len(parsedUrl.Scheme) <= 0 || parsedUrl.Scheme == "http" || parsedUrl.Scheme == "https"
  } else if useHttp {
    _, err := url.Parse(fmt.Sprint("http://", host, ":", port))
    if err != nil {
      fmt.Fprintln(os.Stderr, "Error parsing URL: ", err)
      flag.Usage()
    }
  }
  
  cmd := flag.Arg(0)
  var err error
  if useHttp {
    trans, err = thrift.NewTHttpClient(parsedUrl.String())
    if len(headers) > 0 {
      httptrans := trans.(*thrift.THttpClient)
      for key, value := range headers {
        httptrans.SetHeader(key, value)
      }
    }
  } else {
    portStr := fmt.Sprint(port)
    if strings.Contains(host, ":") {
           host, portStr, err = net.SplitHostPort(host)
           if err != nil {
                   fmt.Fprintln(os.Stderr, "error with host:", err)
                   os.Exit(1)
           }
    }
    trans, err = thrift.NewTSocket(net.JoinHostPort(host, portStr))
    if err != nil {
      fmt.Fprintln(os.Stderr, "error resolving address:", err)
      os.Exit(1)
    }
    if framed {
      trans = thrift.NewTFramedTransport(trans)
    }
  }
  if err != nil {
    fmt.Fprintln(os.Stderr, "Error creating transport", err)
    os.Exit(1)
  }
  defer trans.Close()
  var protocolFactory thrift.TProtocolFactory
  switch protocol {
  case "compact":
    protocolFactory = thrift.NewTCompactProtocolFactory()
    break
  case "simplejson":
    protocolFactory = thrift.NewTSimpleJSONProtocolFactory()
    break
  case "json":
    protocolFactory = thrift.NewTJSONProtocolFactory()
    break
  case "binary", "":
    protocolFactory = thrift.NewTBinaryProtocolFactoryDefault()
    break
  default:
    fmt.Fprintln(os.Stderr, "Invalid protocol specified: ", protocol)
    Usage()
    os.Exit(1)
  }
  iprot := protocolFactory.GetProtocol(trans)
  oprot := protocolFactory.GetProtocol(trans)
  client := executor.NewIGeneralModuleClient(thrift.NewTStandardClient(iprot, oprot))
  if err := trans.Open(); err != nil {
    fmt.Fprintln(os.Stderr, "Error opening socket to ", host, ":", port, " ", err)
    os.Exit(1)
  }
  
  switch cmd {
  case "executeTo":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "ExecuteTo requires 1 args")
      flag.Usage()
    }
    arg80 := flag.Arg(1)
    mbTrans81 := thrift.NewTMemoryBufferLen(len(arg80))
    defer mbTrans81.Close()
    _, err82 := mbTrans81.WriteString(arg80)
    if err82 != nil {
      Usage()
      return
    }
    factory83 := thrift.NewTJSONProtocolFactory()
    jsProt84 := factory83.GetProtocol(mbTrans81)
    argvalue0 := rpc.NewISource()
    err85 := argvalue0.Read(context.Background(), jsProt84)
    if err85 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.ExecuteTo(context.Background(), value0))
    fmt.Print("\n")
    break
  case "map_":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "Map_ requires 1 args")
      flag.Usage()
    }
    arg86 := flag.Arg(1)
    mbTrans87 := thrift.NewTMemoryBufferLen(len(arg86))
    defer mbTrans87.Close()
    _, err88 := mbTrans87.WriteString(arg86)
    if err88 != nil {
      Usage()
      return
    }
    factory89 := thrift.NewTJSONProtocolFactory()
    jsProt90 := factory89.GetProtocol(mbTrans87)
    argvalue0 := rpc.NewISource()
    err91 := argvalue0.Read(context.Background(), jsProt90)
    if err91 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.Map_(context.Background(), value0))
    fmt.Print("\n")
    break
  case "filter":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "Filter requires 1 args")
      flag.Usage()
    }
    arg92 := flag.Arg(1)
    mbTrans93 := thrift.NewTMemoryBufferLen(len(arg92))
    defer mbTrans93.Close()
    _, err94 := mbTrans93.WriteString(arg92)
    if err94 != nil {
      Usage()
      return
    }
    factory95 := thrift.NewTJSONProtocolFactory()
    jsProt96 := factory95.GetProtocol(mbTrans93)
    argvalue0 := rpc.NewISource()
    err97 := argvalue0.Read(context.Background(), jsProt96)
    if err97 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.Filter(context.Background(), value0))
    fmt.Print("\n")
    break
  case "flatmap":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "Flatmap requires 1 args")
      flag.Usage()
    }
    arg98 := flag.Arg(1)
    mbTrans99 := thrift.NewTMemoryBufferLen(len(arg98))
    defer mbTrans99.Close()
    _, err100 := mbTrans99.WriteString(arg98)
    if err100 != nil {
      Usage()
      return
    }
    factory101 := thrift.NewTJSONProtocolFactory()
    jsProt102 := factory101.GetProtocol(mbTrans99)
    argvalue0 := rpc.NewISource()
    err103 := argvalue0.Read(context.Background(), jsProt102)
    if err103 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.Flatmap(context.Background(), value0))
    fmt.Print("\n")
    break
  case "keyBy":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "KeyBy requires 1 args")
      flag.Usage()
    }
    arg104 := flag.Arg(1)
    mbTrans105 := thrift.NewTMemoryBufferLen(len(arg104))
    defer mbTrans105.Close()
    _, err106 := mbTrans105.WriteString(arg104)
    if err106 != nil {
      Usage()
      return
    }
    factory107 := thrift.NewTJSONProtocolFactory()
    jsProt108 := factory107.GetProtocol(mbTrans105)
    argvalue0 := rpc.NewISource()
    err109 := argvalue0.Read(context.Background(), jsProt108)
    if err109 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.KeyBy(context.Background(), value0))
    fmt.Print("\n")
    break
  case "mapPartitions":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "MapPartitions requires 1 args")
      flag.Usage()
    }
    arg110 := flag.Arg(1)
    mbTrans111 := thrift.NewTMemoryBufferLen(len(arg110))
    defer mbTrans111.Close()
    _, err112 := mbTrans111.WriteString(arg110)
    if err112 != nil {
      Usage()
      return
    }
    factory113 := thrift.NewTJSONProtocolFactory()
    jsProt114 := factory113.GetProtocol(mbTrans111)
    argvalue0 := rpc.NewISource()
    err115 := argvalue0.Read(context.Background(), jsProt114)
    if err115 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.MapPartitions(context.Background(), value0))
    fmt.Print("\n")
    break
  case "mapPartitionsWithIndex":
    if flag.NArg() - 1 != 2 {
      fmt.Fprintln(os.Stderr, "MapPartitionsWithIndex requires 2 args")
      flag.Usage()
    }
    arg116 := flag.Arg(1)
    mbTrans117 := thrift.NewTMemoryBufferLen(len(arg116))
    defer mbTrans117.Close()
    _, err118 := mbTrans117.WriteString(arg116)
    if err118 != nil {
      Usage()
      return
    }
    factory119 := thrift.NewTJSONProtocolFactory()
    jsProt120 := factory119.GetProtocol(mbTrans117)
    argvalue0 := rpc.NewISource()
    err121 := argvalue0.Read(context.Background(), jsProt120)
    if err121 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    argvalue1 := flag.Arg(2) == "true"
    value1 := argvalue1
    fmt.Print(client.MapPartitionsWithIndex(context.Background(), value0, value1))
    fmt.Print("\n")
    break
  case "mapExecutor":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "MapExecutor requires 1 args")
      flag.Usage()
    }
    arg123 := flag.Arg(1)
    mbTrans124 := thrift.NewTMemoryBufferLen(len(arg123))
    defer mbTrans124.Close()
    _, err125 := mbTrans124.WriteString(arg123)
    if err125 != nil {
      Usage()
      return
    }
    factory126 := thrift.NewTJSONProtocolFactory()
    jsProt127 := factory126.GetProtocol(mbTrans124)
    argvalue0 := rpc.NewISource()
    err128 := argvalue0.Read(context.Background(), jsProt127)
    if err128 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.MapExecutor(context.Background(), value0))
    fmt.Print("\n")
    break
  case "mapExecutorTo":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "MapExecutorTo requires 1 args")
      flag.Usage()
    }
    arg129 := flag.Arg(1)
    mbTrans130 := thrift.NewTMemoryBufferLen(len(arg129))
    defer mbTrans130.Close()
    _, err131 := mbTrans130.WriteString(arg129)
    if err131 != nil {
      Usage()
      return
    }
    factory132 := thrift.NewTJSONProtocolFactory()
    jsProt133 := factory132.GetProtocol(mbTrans130)
    argvalue0 := rpc.NewISource()
    err134 := argvalue0.Read(context.Background(), jsProt133)
    if err134 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.MapExecutorTo(context.Background(), value0))
    fmt.Print("\n")
    break
  case "groupBy":
    if flag.NArg() - 1 != 2 {
      fmt.Fprintln(os.Stderr, "GroupBy requires 2 args")
      flag.Usage()
    }
    arg135 := flag.Arg(1)
    mbTrans136 := thrift.NewTMemoryBufferLen(len(arg135))
    defer mbTrans136.Close()
    _, err137 := mbTrans136.WriteString(arg135)
    if err137 != nil {
      Usage()
      return
    }
    factory138 := thrift.NewTJSONProtocolFactory()
    jsProt139 := factory138.GetProtocol(mbTrans136)
    argvalue0 := rpc.NewISource()
    err140 := argvalue0.Read(context.Background(), jsProt139)
    if err140 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    argvalue1, err141 := (strconv.ParseInt(flag.Arg(2), 10, 64))
    if err141 != nil {
      Usage()
      return
    }
    value1 := argvalue1
    fmt.Print(client.GroupBy(context.Background(), value0, value1))
    fmt.Print("\n")
    break
  case "sort":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "Sort requires 1 args")
      flag.Usage()
    }
    argvalue0 := flag.Arg(1) == "true"
    value0 := argvalue0
    fmt.Print(client.Sort(context.Background(), value0))
    fmt.Print("\n")
    break
  case "sort2":
    if flag.NArg() - 1 != 2 {
      fmt.Fprintln(os.Stderr, "Sort2 requires 2 args")
      flag.Usage()
    }
    argvalue0 := flag.Arg(1) == "true"
    value0 := argvalue0
    argvalue1, err144 := (strconv.ParseInt(flag.Arg(2), 10, 64))
    if err144 != nil {
      Usage()
      return
    }
    value1 := argvalue1
    fmt.Print(client.Sort2(context.Background(), value0, value1))
    fmt.Print("\n")
    break
  case "sortBy":
    if flag.NArg() - 1 != 2 {
      fmt.Fprintln(os.Stderr, "SortBy requires 2 args")
      flag.Usage()
    }
    arg145 := flag.Arg(1)
    mbTrans146 := thrift.NewTMemoryBufferLen(len(arg145))
    defer mbTrans146.Close()
    _, err147 := mbTrans146.WriteString(arg145)
    if err147 != nil {
      Usage()
      return
    }
    factory148 := thrift.NewTJSONProtocolFactory()
    jsProt149 := factory148.GetProtocol(mbTrans146)
    argvalue0 := rpc.NewISource()
    err150 := argvalue0.Read(context.Background(), jsProt149)
    if err150 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    argvalue1 := flag.Arg(2) == "true"
    value1 := argvalue1
    fmt.Print(client.SortBy(context.Background(), value0, value1))
    fmt.Print("\n")
    break
  case "sortBy3":
    if flag.NArg() - 1 != 3 {
      fmt.Fprintln(os.Stderr, "SortBy3 requires 3 args")
      flag.Usage()
    }
    arg152 := flag.Arg(1)
    mbTrans153 := thrift.NewTMemoryBufferLen(len(arg152))
    defer mbTrans153.Close()
    _, err154 := mbTrans153.WriteString(arg152)
    if err154 != nil {
      Usage()
      return
    }
    factory155 := thrift.NewTJSONProtocolFactory()
    jsProt156 := factory155.GetProtocol(mbTrans153)
    argvalue0 := rpc.NewISource()
    err157 := argvalue0.Read(context.Background(), jsProt156)
    if err157 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    argvalue1 := flag.Arg(2) == "true"
    value1 := argvalue1
    argvalue2, err159 := (strconv.ParseInt(flag.Arg(3), 10, 64))
    if err159 != nil {
      Usage()
      return
    }
    value2 := argvalue2
    fmt.Print(client.SortBy3(context.Background(), value0, value1, value2))
    fmt.Print("\n")
    break
  case "flatMapValues":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "FlatMapValues requires 1 args")
      flag.Usage()
    }
    arg160 := flag.Arg(1)
    mbTrans161 := thrift.NewTMemoryBufferLen(len(arg160))
    defer mbTrans161.Close()
    _, err162 := mbTrans161.WriteString(arg160)
    if err162 != nil {
      Usage()
      return
    }
    factory163 := thrift.NewTJSONProtocolFactory()
    jsProt164 := factory163.GetProtocol(mbTrans161)
    argvalue0 := rpc.NewISource()
    err165 := argvalue0.Read(context.Background(), jsProt164)
    if err165 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.FlatMapValues(context.Background(), value0))
    fmt.Print("\n")
    break
  case "mapValues":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "MapValues requires 1 args")
      flag.Usage()
    }
    arg166 := flag.Arg(1)
    mbTrans167 := thrift.NewTMemoryBufferLen(len(arg166))
    defer mbTrans167.Close()
    _, err168 := mbTrans167.WriteString(arg166)
    if err168 != nil {
      Usage()
      return
    }
    factory169 := thrift.NewTJSONProtocolFactory()
    jsProt170 := factory169.GetProtocol(mbTrans167)
    argvalue0 := rpc.NewISource()
    err171 := argvalue0.Read(context.Background(), jsProt170)
    if err171 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.MapValues(context.Background(), value0))
    fmt.Print("\n")
    break
  case "groupByKey":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "GroupByKey requires 1 args")
      flag.Usage()
    }
    argvalue0, err172 := (strconv.ParseInt(flag.Arg(1), 10, 64))
    if err172 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.GroupByKey(context.Background(), value0))
    fmt.Print("\n")
    break
  case "groupByKey2":
    if flag.NArg() - 1 != 2 {
      fmt.Fprintln(os.Stderr, "GroupByKey2 requires 2 args")
      flag.Usage()
    }
    argvalue0, err173 := (strconv.ParseInt(flag.Arg(1), 10, 64))
    if err173 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    arg174 := flag.Arg(2)
    mbTrans175 := thrift.NewTMemoryBufferLen(len(arg174))
    defer mbTrans175.Close()
    _, err176 := mbTrans175.WriteString(arg174)
    if err176 != nil {
      Usage()
      return
    }
    factory177 := thrift.NewTJSONProtocolFactory()
    jsProt178 := factory177.GetProtocol(mbTrans175)
    argvalue1 := rpc.NewISource()
    err179 := argvalue1.Read(context.Background(), jsProt178)
    if err179 != nil {
      Usage()
      return
    }
    value1 := argvalue1
    fmt.Print(client.GroupByKey2(context.Background(), value0, value1))
    fmt.Print("\n")
    break
  case "reduceByKey":
    if flag.NArg() - 1 != 3 {
      fmt.Fprintln(os.Stderr, "ReduceByKey requires 3 args")
      flag.Usage()
    }
    arg180 := flag.Arg(1)
    mbTrans181 := thrift.NewTMemoryBufferLen(len(arg180))
    defer mbTrans181.Close()
    _, err182 := mbTrans181.WriteString(arg180)
    if err182 != nil {
      Usage()
      return
    }
    factory183 := thrift.NewTJSONProtocolFactory()
    jsProt184 := factory183.GetProtocol(mbTrans181)
    argvalue0 := rpc.NewISource()
    err185 := argvalue0.Read(context.Background(), jsProt184)
    if err185 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    argvalue1, err186 := (strconv.ParseInt(flag.Arg(2), 10, 64))
    if err186 != nil {
      Usage()
      return
    }
    value1 := argvalue1
    argvalue2 := flag.Arg(3) == "true"
    value2 := argvalue2
    fmt.Print(client.ReduceByKey(context.Background(), value0, value1, value2))
    fmt.Print("\n")
    break
  case "aggregateByKey":
    if flag.NArg() - 1 != 3 {
      fmt.Fprintln(os.Stderr, "AggregateByKey requires 3 args")
      flag.Usage()
    }
    arg188 := flag.Arg(1)
    mbTrans189 := thrift.NewTMemoryBufferLen(len(arg188))
    defer mbTrans189.Close()
    _, err190 := mbTrans189.WriteString(arg188)
    if err190 != nil {
      Usage()
      return
    }
    factory191 := thrift.NewTJSONProtocolFactory()
    jsProt192 := factory191.GetProtocol(mbTrans189)
    argvalue0 := rpc.NewISource()
    err193 := argvalue0.Read(context.Background(), jsProt192)
    if err193 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    arg194 := flag.Arg(2)
    mbTrans195 := thrift.NewTMemoryBufferLen(len(arg194))
    defer mbTrans195.Close()
    _, err196 := mbTrans195.WriteString(arg194)
    if err196 != nil {
      Usage()
      return
    }
    factory197 := thrift.NewTJSONProtocolFactory()
    jsProt198 := factory197.GetProtocol(mbTrans195)
    argvalue1 := rpc.NewISource()
    err199 := argvalue1.Read(context.Background(), jsProt198)
    if err199 != nil {
      Usage()
      return
    }
    value1 := argvalue1
    argvalue2, err200 := (strconv.ParseInt(flag.Arg(3), 10, 64))
    if err200 != nil {
      Usage()
      return
    }
    value2 := argvalue2
    fmt.Print(client.AggregateByKey(context.Background(), value0, value1, value2))
    fmt.Print("\n")
    break
  case "aggregateByKey4":
    if flag.NArg() - 1 != 4 {
      fmt.Fprintln(os.Stderr, "AggregateByKey4 requires 4 args")
      flag.Usage()
    }
    arg201 := flag.Arg(1)
    mbTrans202 := thrift.NewTMemoryBufferLen(len(arg201))
    defer mbTrans202.Close()
    _, err203 := mbTrans202.WriteString(arg201)
    if err203 != nil {
      Usage()
      return
    }
    factory204 := thrift.NewTJSONProtocolFactory()
    jsProt205 := factory204.GetProtocol(mbTrans202)
    argvalue0 := rpc.NewISource()
    err206 := argvalue0.Read(context.Background(), jsProt205)
    if err206 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    arg207 := flag.Arg(2)
    mbTrans208 := thrift.NewTMemoryBufferLen(len(arg207))
    defer mbTrans208.Close()
    _, err209 := mbTrans208.WriteString(arg207)
    if err209 != nil {
      Usage()
      return
    }
    factory210 := thrift.NewTJSONProtocolFactory()
    jsProt211 := factory210.GetProtocol(mbTrans208)
    argvalue1 := rpc.NewISource()
    err212 := argvalue1.Read(context.Background(), jsProt211)
    if err212 != nil {
      Usage()
      return
    }
    value1 := argvalue1
    arg213 := flag.Arg(3)
    mbTrans214 := thrift.NewTMemoryBufferLen(len(arg213))
    defer mbTrans214.Close()
    _, err215 := mbTrans214.WriteString(arg213)
    if err215 != nil {
      Usage()
      return
    }
    factory216 := thrift.NewTJSONProtocolFactory()
    jsProt217 := factory216.GetProtocol(mbTrans214)
    argvalue2 := rpc.NewISource()
    err218 := argvalue2.Read(context.Background(), jsProt217)
    if err218 != nil {
      Usage()
      return
    }
    value2 := argvalue2
    argvalue3, err219 := (strconv.ParseInt(flag.Arg(4), 10, 64))
    if err219 != nil {
      Usage()
      return
    }
    value3 := argvalue3
    fmt.Print(client.AggregateByKey4(context.Background(), value0, value1, value2, value3))
    fmt.Print("\n")
    break
  case "foldByKey":
    if flag.NArg() - 1 != 4 {
      fmt.Fprintln(os.Stderr, "FoldByKey requires 4 args")
      flag.Usage()
    }
    arg220 := flag.Arg(1)
    mbTrans221 := thrift.NewTMemoryBufferLen(len(arg220))
    defer mbTrans221.Close()
    _, err222 := mbTrans221.WriteString(arg220)
    if err222 != nil {
      Usage()
      return
    }
    factory223 := thrift.NewTJSONProtocolFactory()
    jsProt224 := factory223.GetProtocol(mbTrans221)
    argvalue0 := rpc.NewISource()
    err225 := argvalue0.Read(context.Background(), jsProt224)
    if err225 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    arg226 := flag.Arg(2)
    mbTrans227 := thrift.NewTMemoryBufferLen(len(arg226))
    defer mbTrans227.Close()
    _, err228 := mbTrans227.WriteString(arg226)
    if err228 != nil {
      Usage()
      return
    }
    factory229 := thrift.NewTJSONProtocolFactory()
    jsProt230 := factory229.GetProtocol(mbTrans227)
    argvalue1 := rpc.NewISource()
    err231 := argvalue1.Read(context.Background(), jsProt230)
    if err231 != nil {
      Usage()
      return
    }
    value1 := argvalue1
    argvalue2, err232 := (strconv.ParseInt(flag.Arg(3), 10, 64))
    if err232 != nil {
      Usage()
      return
    }
    value2 := argvalue2
    argvalue3 := flag.Arg(4) == "true"
    value3 := argvalue3
    fmt.Print(client.FoldByKey(context.Background(), value0, value1, value2, value3))
    fmt.Print("\n")
    break
  case "sortByKey":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "SortByKey requires 1 args")
      flag.Usage()
    }
    argvalue0 := flag.Arg(1) == "true"
    value0 := argvalue0
    fmt.Print(client.SortByKey(context.Background(), value0))
    fmt.Print("\n")
    break
  case "sortByKey2a":
    if flag.NArg() - 1 != 2 {
      fmt.Fprintln(os.Stderr, "SortByKey2a requires 2 args")
      flag.Usage()
    }
    argvalue0 := flag.Arg(1) == "true"
    value0 := argvalue0
    argvalue1, err236 := (strconv.ParseInt(flag.Arg(2), 10, 64))
    if err236 != nil {
      Usage()
      return
    }
    value1 := argvalue1
    fmt.Print(client.SortByKey2a(context.Background(), value0, value1))
    fmt.Print("\n")
    break
  case "sortByKey2b":
    if flag.NArg() - 1 != 2 {
      fmt.Fprintln(os.Stderr, "SortByKey2b requires 2 args")
      flag.Usage()
    }
    arg237 := flag.Arg(1)
    mbTrans238 := thrift.NewTMemoryBufferLen(len(arg237))
    defer mbTrans238.Close()
    _, err239 := mbTrans238.WriteString(arg237)
    if err239 != nil {
      Usage()
      return
    }
    factory240 := thrift.NewTJSONProtocolFactory()
    jsProt241 := factory240.GetProtocol(mbTrans238)
    argvalue0 := rpc.NewISource()
    err242 := argvalue0.Read(context.Background(), jsProt241)
    if err242 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    argvalue1 := flag.Arg(2) == "true"
    value1 := argvalue1
    fmt.Print(client.SortByKey2b(context.Background(), value0, value1))
    fmt.Print("\n")
    break
  case "sortByKey3":
    if flag.NArg() - 1 != 3 {
      fmt.Fprintln(os.Stderr, "SortByKey3 requires 3 args")
      flag.Usage()
    }
    arg244 := flag.Arg(1)
    mbTrans245 := thrift.NewTMemoryBufferLen(len(arg244))
    defer mbTrans245.Close()
    _, err246 := mbTrans245.WriteString(arg244)
    if err246 != nil {
      Usage()
      return
    }
    factory247 := thrift.NewTJSONProtocolFactory()
    jsProt248 := factory247.GetProtocol(mbTrans245)
    argvalue0 := rpc.NewISource()
    err249 := argvalue0.Read(context.Background(), jsProt248)
    if err249 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    argvalue1 := flag.Arg(2) == "true"
    value1 := argvalue1
    argvalue2, err251 := (strconv.ParseInt(flag.Arg(3), 10, 64))
    if err251 != nil {
      Usage()
      return
    }
    value2 := argvalue2
    fmt.Print(client.SortByKey3(context.Background(), value0, value1, value2))
    fmt.Print("\n")
    break
  case "":
    Usage()
    break
  default:
    fmt.Fprintln(os.Stderr, "Invalid function ", cmd)
  }
}
