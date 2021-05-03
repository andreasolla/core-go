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
  fmt.Fprintln(os.Stderr, "  void execute(ISource src)")
  fmt.Fprintln(os.Stderr, "  void reduce(ISource src)")
  fmt.Fprintln(os.Stderr, "  void treeReduce(ISource src)")
  fmt.Fprintln(os.Stderr, "  void collect()")
  fmt.Fprintln(os.Stderr, "  void aggregate(ISource zero, ISource seqOp, ISource combOp)")
  fmt.Fprintln(os.Stderr, "  void treeAggregate(ISource zero, ISource seqOp, ISource combOp)")
  fmt.Fprintln(os.Stderr, "  void fold(ISource zero, ISource src)")
  fmt.Fprintln(os.Stderr, "  void treeFold(ISource zero, ISource src)")
  fmt.Fprintln(os.Stderr, "  void take(i64 num)")
  fmt.Fprintln(os.Stderr, "  void foreach_(ISource src)")
  fmt.Fprintln(os.Stderr, "  void foreachPartition(ISource src)")
  fmt.Fprintln(os.Stderr, "  void foreachExecutor(ISource src)")
  fmt.Fprintln(os.Stderr, "  void top(i64 num)")
  fmt.Fprintln(os.Stderr, "  void top2(i64 num, ISource cmp)")
  fmt.Fprintln(os.Stderr, "  void takeOrdered(i64 num)")
  fmt.Fprintln(os.Stderr, "  void takeOrdered2(i64 num, ISource cmp)")
  fmt.Fprintln(os.Stderr, "  void keys()")
  fmt.Fprintln(os.Stderr, "  void values()")
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
  client := executor.NewIGeneralActionModuleClient(thrift.NewTStandardClient(iprot, oprot))
  if err := trans.Open(); err != nil {
    fmt.Fprintln(os.Stderr, "Error opening socket to ", host, ":", port, " ", err)
    os.Exit(1)
  }
  
  switch cmd {
  case "execute":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "Execute requires 1 args")
      flag.Usage()
    }
    arg56 := flag.Arg(1)
    mbTrans57 := thrift.NewTMemoryBufferLen(len(arg56))
    defer mbTrans57.Close()
    _, err58 := mbTrans57.WriteString(arg56)
    if err58 != nil {
      Usage()
      return
    }
    factory59 := thrift.NewTJSONProtocolFactory()
    jsProt60 := factory59.GetProtocol(mbTrans57)
    argvalue0 := rpc.NewISource()
    err61 := argvalue0.Read(context.Background(), jsProt60)
    if err61 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.Execute(context.Background(), value0))
    fmt.Print("\n")
    break
  case "reduce":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "Reduce requires 1 args")
      flag.Usage()
    }
    arg62 := flag.Arg(1)
    mbTrans63 := thrift.NewTMemoryBufferLen(len(arg62))
    defer mbTrans63.Close()
    _, err64 := mbTrans63.WriteString(arg62)
    if err64 != nil {
      Usage()
      return
    }
    factory65 := thrift.NewTJSONProtocolFactory()
    jsProt66 := factory65.GetProtocol(mbTrans63)
    argvalue0 := rpc.NewISource()
    err67 := argvalue0.Read(context.Background(), jsProt66)
    if err67 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.Reduce(context.Background(), value0))
    fmt.Print("\n")
    break
  case "treeReduce":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "TreeReduce requires 1 args")
      flag.Usage()
    }
    arg68 := flag.Arg(1)
    mbTrans69 := thrift.NewTMemoryBufferLen(len(arg68))
    defer mbTrans69.Close()
    _, err70 := mbTrans69.WriteString(arg68)
    if err70 != nil {
      Usage()
      return
    }
    factory71 := thrift.NewTJSONProtocolFactory()
    jsProt72 := factory71.GetProtocol(mbTrans69)
    argvalue0 := rpc.NewISource()
    err73 := argvalue0.Read(context.Background(), jsProt72)
    if err73 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.TreeReduce(context.Background(), value0))
    fmt.Print("\n")
    break
  case "collect":
    if flag.NArg() - 1 != 0 {
      fmt.Fprintln(os.Stderr, "Collect requires 0 args")
      flag.Usage()
    }
    fmt.Print(client.Collect(context.Background()))
    fmt.Print("\n")
    break
  case "aggregate":
    if flag.NArg() - 1 != 3 {
      fmt.Fprintln(os.Stderr, "Aggregate requires 3 args")
      flag.Usage()
    }
    arg74 := flag.Arg(1)
    mbTrans75 := thrift.NewTMemoryBufferLen(len(arg74))
    defer mbTrans75.Close()
    _, err76 := mbTrans75.WriteString(arg74)
    if err76 != nil {
      Usage()
      return
    }
    factory77 := thrift.NewTJSONProtocolFactory()
    jsProt78 := factory77.GetProtocol(mbTrans75)
    argvalue0 := rpc.NewISource()
    err79 := argvalue0.Read(context.Background(), jsProt78)
    if err79 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    arg80 := flag.Arg(2)
    mbTrans81 := thrift.NewTMemoryBufferLen(len(arg80))
    defer mbTrans81.Close()
    _, err82 := mbTrans81.WriteString(arg80)
    if err82 != nil {
      Usage()
      return
    }
    factory83 := thrift.NewTJSONProtocolFactory()
    jsProt84 := factory83.GetProtocol(mbTrans81)
    argvalue1 := rpc.NewISource()
    err85 := argvalue1.Read(context.Background(), jsProt84)
    if err85 != nil {
      Usage()
      return
    }
    value1 := argvalue1
    arg86 := flag.Arg(3)
    mbTrans87 := thrift.NewTMemoryBufferLen(len(arg86))
    defer mbTrans87.Close()
    _, err88 := mbTrans87.WriteString(arg86)
    if err88 != nil {
      Usage()
      return
    }
    factory89 := thrift.NewTJSONProtocolFactory()
    jsProt90 := factory89.GetProtocol(mbTrans87)
    argvalue2 := rpc.NewISource()
    err91 := argvalue2.Read(context.Background(), jsProt90)
    if err91 != nil {
      Usage()
      return
    }
    value2 := argvalue2
    fmt.Print(client.Aggregate(context.Background(), value0, value1, value2))
    fmt.Print("\n")
    break
  case "treeAggregate":
    if flag.NArg() - 1 != 3 {
      fmt.Fprintln(os.Stderr, "TreeAggregate requires 3 args")
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
    arg98 := flag.Arg(2)
    mbTrans99 := thrift.NewTMemoryBufferLen(len(arg98))
    defer mbTrans99.Close()
    _, err100 := mbTrans99.WriteString(arg98)
    if err100 != nil {
      Usage()
      return
    }
    factory101 := thrift.NewTJSONProtocolFactory()
    jsProt102 := factory101.GetProtocol(mbTrans99)
    argvalue1 := rpc.NewISource()
    err103 := argvalue1.Read(context.Background(), jsProt102)
    if err103 != nil {
      Usage()
      return
    }
    value1 := argvalue1
    arg104 := flag.Arg(3)
    mbTrans105 := thrift.NewTMemoryBufferLen(len(arg104))
    defer mbTrans105.Close()
    _, err106 := mbTrans105.WriteString(arg104)
    if err106 != nil {
      Usage()
      return
    }
    factory107 := thrift.NewTJSONProtocolFactory()
    jsProt108 := factory107.GetProtocol(mbTrans105)
    argvalue2 := rpc.NewISource()
    err109 := argvalue2.Read(context.Background(), jsProt108)
    if err109 != nil {
      Usage()
      return
    }
    value2 := argvalue2
    fmt.Print(client.TreeAggregate(context.Background(), value0, value1, value2))
    fmt.Print("\n")
    break
  case "fold":
    if flag.NArg() - 1 != 2 {
      fmt.Fprintln(os.Stderr, "Fold requires 2 args")
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
    arg116 := flag.Arg(2)
    mbTrans117 := thrift.NewTMemoryBufferLen(len(arg116))
    defer mbTrans117.Close()
    _, err118 := mbTrans117.WriteString(arg116)
    if err118 != nil {
      Usage()
      return
    }
    factory119 := thrift.NewTJSONProtocolFactory()
    jsProt120 := factory119.GetProtocol(mbTrans117)
    argvalue1 := rpc.NewISource()
    err121 := argvalue1.Read(context.Background(), jsProt120)
    if err121 != nil {
      Usage()
      return
    }
    value1 := argvalue1
    fmt.Print(client.Fold(context.Background(), value0, value1))
    fmt.Print("\n")
    break
  case "treeFold":
    if flag.NArg() - 1 != 2 {
      fmt.Fprintln(os.Stderr, "TreeFold requires 2 args")
      flag.Usage()
    }
    arg122 := flag.Arg(1)
    mbTrans123 := thrift.NewTMemoryBufferLen(len(arg122))
    defer mbTrans123.Close()
    _, err124 := mbTrans123.WriteString(arg122)
    if err124 != nil {
      Usage()
      return
    }
    factory125 := thrift.NewTJSONProtocolFactory()
    jsProt126 := factory125.GetProtocol(mbTrans123)
    argvalue0 := rpc.NewISource()
    err127 := argvalue0.Read(context.Background(), jsProt126)
    if err127 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    arg128 := flag.Arg(2)
    mbTrans129 := thrift.NewTMemoryBufferLen(len(arg128))
    defer mbTrans129.Close()
    _, err130 := mbTrans129.WriteString(arg128)
    if err130 != nil {
      Usage()
      return
    }
    factory131 := thrift.NewTJSONProtocolFactory()
    jsProt132 := factory131.GetProtocol(mbTrans129)
    argvalue1 := rpc.NewISource()
    err133 := argvalue1.Read(context.Background(), jsProt132)
    if err133 != nil {
      Usage()
      return
    }
    value1 := argvalue1
    fmt.Print(client.TreeFold(context.Background(), value0, value1))
    fmt.Print("\n")
    break
  case "take":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "Take requires 1 args")
      flag.Usage()
    }
    argvalue0, err134 := (strconv.ParseInt(flag.Arg(1), 10, 64))
    if err134 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.Take(context.Background(), value0))
    fmt.Print("\n")
    break
  case "foreach_":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "Foreach_ requires 1 args")
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
    fmt.Print(client.Foreach_(context.Background(), value0))
    fmt.Print("\n")
    break
  case "foreachPartition":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "ForeachPartition requires 1 args")
      flag.Usage()
    }
    arg141 := flag.Arg(1)
    mbTrans142 := thrift.NewTMemoryBufferLen(len(arg141))
    defer mbTrans142.Close()
    _, err143 := mbTrans142.WriteString(arg141)
    if err143 != nil {
      Usage()
      return
    }
    factory144 := thrift.NewTJSONProtocolFactory()
    jsProt145 := factory144.GetProtocol(mbTrans142)
    argvalue0 := rpc.NewISource()
    err146 := argvalue0.Read(context.Background(), jsProt145)
    if err146 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.ForeachPartition(context.Background(), value0))
    fmt.Print("\n")
    break
  case "foreachExecutor":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "ForeachExecutor requires 1 args")
      flag.Usage()
    }
    arg147 := flag.Arg(1)
    mbTrans148 := thrift.NewTMemoryBufferLen(len(arg147))
    defer mbTrans148.Close()
    _, err149 := mbTrans148.WriteString(arg147)
    if err149 != nil {
      Usage()
      return
    }
    factory150 := thrift.NewTJSONProtocolFactory()
    jsProt151 := factory150.GetProtocol(mbTrans148)
    argvalue0 := rpc.NewISource()
    err152 := argvalue0.Read(context.Background(), jsProt151)
    if err152 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.ForeachExecutor(context.Background(), value0))
    fmt.Print("\n")
    break
  case "top":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "Top requires 1 args")
      flag.Usage()
    }
    argvalue0, err153 := (strconv.ParseInt(flag.Arg(1), 10, 64))
    if err153 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.Top(context.Background(), value0))
    fmt.Print("\n")
    break
  case "top2":
    if flag.NArg() - 1 != 2 {
      fmt.Fprintln(os.Stderr, "Top2 requires 2 args")
      flag.Usage()
    }
    argvalue0, err154 := (strconv.ParseInt(flag.Arg(1), 10, 64))
    if err154 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    arg155 := flag.Arg(2)
    mbTrans156 := thrift.NewTMemoryBufferLen(len(arg155))
    defer mbTrans156.Close()
    _, err157 := mbTrans156.WriteString(arg155)
    if err157 != nil {
      Usage()
      return
    }
    factory158 := thrift.NewTJSONProtocolFactory()
    jsProt159 := factory158.GetProtocol(mbTrans156)
    argvalue1 := rpc.NewISource()
    err160 := argvalue1.Read(context.Background(), jsProt159)
    if err160 != nil {
      Usage()
      return
    }
    value1 := argvalue1
    fmt.Print(client.Top2(context.Background(), value0, value1))
    fmt.Print("\n")
    break
  case "takeOrdered":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "TakeOrdered requires 1 args")
      flag.Usage()
    }
    argvalue0, err161 := (strconv.ParseInt(flag.Arg(1), 10, 64))
    if err161 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.TakeOrdered(context.Background(), value0))
    fmt.Print("\n")
    break
  case "takeOrdered2":
    if flag.NArg() - 1 != 2 {
      fmt.Fprintln(os.Stderr, "TakeOrdered2 requires 2 args")
      flag.Usage()
    }
    argvalue0, err162 := (strconv.ParseInt(flag.Arg(1), 10, 64))
    if err162 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    arg163 := flag.Arg(2)
    mbTrans164 := thrift.NewTMemoryBufferLen(len(arg163))
    defer mbTrans164.Close()
    _, err165 := mbTrans164.WriteString(arg163)
    if err165 != nil {
      Usage()
      return
    }
    factory166 := thrift.NewTJSONProtocolFactory()
    jsProt167 := factory166.GetProtocol(mbTrans164)
    argvalue1 := rpc.NewISource()
    err168 := argvalue1.Read(context.Background(), jsProt167)
    if err168 != nil {
      Usage()
      return
    }
    value1 := argvalue1
    fmt.Print(client.TakeOrdered2(context.Background(), value0, value1))
    fmt.Print("\n")
    break
  case "keys":
    if flag.NArg() - 1 != 0 {
      fmt.Fprintln(os.Stderr, "Keys requires 0 args")
      flag.Usage()
    }
    fmt.Print(client.Keys(context.Background()))
    fmt.Print("\n")
    break
  case "values":
    if flag.NArg() - 1 != 0 {
      fmt.Fprintln(os.Stderr, "Values requires 0 args")
      flag.Usage()
    }
    fmt.Print(client.Values(context.Background()))
    fmt.Print("\n")
    break
  case "":
    Usage()
    break
  default:
    fmt.Fprintln(os.Stderr, "Invalid function ", cmd)
  }
}
