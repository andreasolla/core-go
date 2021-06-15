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
  fmt.Fprintln(os.Stderr, "  void loadClass(ISource src)")
  fmt.Fprintln(os.Stderr, "  void loadLibrary(string path)")
  fmt.Fprintln(os.Stderr, "  i64 partitionCount()")
  fmt.Fprintln(os.Stderr, "   countByPartition()")
  fmt.Fprintln(os.Stderr, "  i64 partitionApproxSize()")
  fmt.Fprintln(os.Stderr, "  void textFile(string path)")
  fmt.Fprintln(os.Stderr, "  void textFile2(string path, i64 minPartitions)")
  fmt.Fprintln(os.Stderr, "  void partitionObjectFile(string path, i64 first, i64 partitions)")
  fmt.Fprintln(os.Stderr, "  void partitionObjectFile4(string path, i64 first, i64 partitions, ISource src)")
  fmt.Fprintln(os.Stderr, "  void partitionTextFile(string path, i64 first, i64 partitions)")
  fmt.Fprintln(os.Stderr, "  void partitionJsonFile4a(string path, i64 first, i64 partitions, bool objectMapping)")
  fmt.Fprintln(os.Stderr, "  void partitionJsonFile4b(string path, i64 first, i64 partitions, ISource src)")
  fmt.Fprintln(os.Stderr, "  void saveAsObjectFile(string path, i8 compression, i64 first)")
  fmt.Fprintln(os.Stderr, "  void saveAsTextFile(string path, i64 first)")
  fmt.Fprintln(os.Stderr, "  void saveAsJsonFile(string path, i64 first, bool pretty)")
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
  client := executor.NewIIOModuleClient(thrift.NewTStandardClient(iprot, oprot))
  if err := trans.Open(); err != nil {
    fmt.Fprintln(os.Stderr, "Error opening socket to ", host, ":", port, " ", err)
    os.Exit(1)
  }
  
  switch cmd {
  case "loadClass":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "LoadClass requires 1 args")
      flag.Usage()
    }
    arg48 := flag.Arg(1)
    mbTrans49 := thrift.NewTMemoryBufferLen(len(arg48))
    defer mbTrans49.Close()
    _, err50 := mbTrans49.WriteString(arg48)
    if err50 != nil {
      Usage()
      return
    }
    factory51 := thrift.NewTJSONProtocolFactory()
    jsProt52 := factory51.GetProtocol(mbTrans49)
    argvalue0 := rpc.NewISource()
    err53 := argvalue0.Read(context.Background(), jsProt52)
    if err53 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.LoadClass(context.Background(), value0))
    fmt.Print("\n")
    break
  case "loadLibrary":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "LoadLibrary requires 1 args")
      flag.Usage()
    }
    argvalue0 := flag.Arg(1)
    value0 := argvalue0
    fmt.Print(client.LoadLibrary(context.Background(), value0))
    fmt.Print("\n")
    break
  case "partitionCount":
    if flag.NArg() - 1 != 0 {
      fmt.Fprintln(os.Stderr, "PartitionCount requires 0 args")
      flag.Usage()
    }
    fmt.Print(client.PartitionCount(context.Background()))
    fmt.Print("\n")
    break
  case "countByPartition":
    if flag.NArg() - 1 != 0 {
      fmt.Fprintln(os.Stderr, "CountByPartition requires 0 args")
      flag.Usage()
    }
    fmt.Print(client.CountByPartition(context.Background()))
    fmt.Print("\n")
    break
  case "partitionApproxSize":
    if flag.NArg() - 1 != 0 {
      fmt.Fprintln(os.Stderr, "PartitionApproxSize requires 0 args")
      flag.Usage()
    }
    fmt.Print(client.PartitionApproxSize(context.Background()))
    fmt.Print("\n")
    break
  case "textFile":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "TextFile requires 1 args")
      flag.Usage()
    }
    argvalue0 := flag.Arg(1)
    value0 := argvalue0
    fmt.Print(client.TextFile(context.Background(), value0))
    fmt.Print("\n")
    break
  case "textFile2":
    if flag.NArg() - 1 != 2 {
      fmt.Fprintln(os.Stderr, "TextFile2 requires 2 args")
      flag.Usage()
    }
    argvalue0 := flag.Arg(1)
    value0 := argvalue0
    argvalue1, err57 := (strconv.ParseInt(flag.Arg(2), 10, 64))
    if err57 != nil {
      Usage()
      return
    }
    value1 := argvalue1
    fmt.Print(client.TextFile2(context.Background(), value0, value1))
    fmt.Print("\n")
    break
  case "partitionObjectFile":
    if flag.NArg() - 1 != 3 {
      fmt.Fprintln(os.Stderr, "PartitionObjectFile requires 3 args")
      flag.Usage()
    }
    argvalue0 := flag.Arg(1)
    value0 := argvalue0
    argvalue1, err59 := (strconv.ParseInt(flag.Arg(2), 10, 64))
    if err59 != nil {
      Usage()
      return
    }
    value1 := argvalue1
    argvalue2, err60 := (strconv.ParseInt(flag.Arg(3), 10, 64))
    if err60 != nil {
      Usage()
      return
    }
    value2 := argvalue2
    fmt.Print(client.PartitionObjectFile(context.Background(), value0, value1, value2))
    fmt.Print("\n")
    break
  case "partitionObjectFile4":
    if flag.NArg() - 1 != 4 {
      fmt.Fprintln(os.Stderr, "PartitionObjectFile4 requires 4 args")
      flag.Usage()
    }
    argvalue0 := flag.Arg(1)
    value0 := argvalue0
    argvalue1, err62 := (strconv.ParseInt(flag.Arg(2), 10, 64))
    if err62 != nil {
      Usage()
      return
    }
    value1 := argvalue1
    argvalue2, err63 := (strconv.ParseInt(flag.Arg(3), 10, 64))
    if err63 != nil {
      Usage()
      return
    }
    value2 := argvalue2
    arg64 := flag.Arg(4)
    mbTrans65 := thrift.NewTMemoryBufferLen(len(arg64))
    defer mbTrans65.Close()
    _, err66 := mbTrans65.WriteString(arg64)
    if err66 != nil {
      Usage()
      return
    }
    factory67 := thrift.NewTJSONProtocolFactory()
    jsProt68 := factory67.GetProtocol(mbTrans65)
    argvalue3 := rpc.NewISource()
    err69 := argvalue3.Read(context.Background(), jsProt68)
    if err69 != nil {
      Usage()
      return
    }
    value3 := argvalue3
    fmt.Print(client.PartitionObjectFile4(context.Background(), value0, value1, value2, value3))
    fmt.Print("\n")
    break
  case "partitionTextFile":
    if flag.NArg() - 1 != 3 {
      fmt.Fprintln(os.Stderr, "PartitionTextFile requires 3 args")
      flag.Usage()
    }
    argvalue0 := flag.Arg(1)
    value0 := argvalue0
    argvalue1, err71 := (strconv.ParseInt(flag.Arg(2), 10, 64))
    if err71 != nil {
      Usage()
      return
    }
    value1 := argvalue1
    argvalue2, err72 := (strconv.ParseInt(flag.Arg(3), 10, 64))
    if err72 != nil {
      Usage()
      return
    }
    value2 := argvalue2
    fmt.Print(client.PartitionTextFile(context.Background(), value0, value1, value2))
    fmt.Print("\n")
    break
  case "partitionJsonFile4a":
    if flag.NArg() - 1 != 4 {
      fmt.Fprintln(os.Stderr, "PartitionJsonFile4a requires 4 args")
      flag.Usage()
    }
    argvalue0 := flag.Arg(1)
    value0 := argvalue0
    argvalue1, err74 := (strconv.ParseInt(flag.Arg(2), 10, 64))
    if err74 != nil {
      Usage()
      return
    }
    value1 := argvalue1
    argvalue2, err75 := (strconv.ParseInt(flag.Arg(3), 10, 64))
    if err75 != nil {
      Usage()
      return
    }
    value2 := argvalue2
    argvalue3 := flag.Arg(4) == "true"
    value3 := argvalue3
    fmt.Print(client.PartitionJsonFile4a(context.Background(), value0, value1, value2, value3))
    fmt.Print("\n")
    break
  case "partitionJsonFile4b":
    if flag.NArg() - 1 != 4 {
      fmt.Fprintln(os.Stderr, "PartitionJsonFile4b requires 4 args")
      flag.Usage()
    }
    argvalue0 := flag.Arg(1)
    value0 := argvalue0
    argvalue1, err78 := (strconv.ParseInt(flag.Arg(2), 10, 64))
    if err78 != nil {
      Usage()
      return
    }
    value1 := argvalue1
    argvalue2, err79 := (strconv.ParseInt(flag.Arg(3), 10, 64))
    if err79 != nil {
      Usage()
      return
    }
    value2 := argvalue2
    arg80 := flag.Arg(4)
    mbTrans81 := thrift.NewTMemoryBufferLen(len(arg80))
    defer mbTrans81.Close()
    _, err82 := mbTrans81.WriteString(arg80)
    if err82 != nil {
      Usage()
      return
    }
    factory83 := thrift.NewTJSONProtocolFactory()
    jsProt84 := factory83.GetProtocol(mbTrans81)
    argvalue3 := rpc.NewISource()
    err85 := argvalue3.Read(context.Background(), jsProt84)
    if err85 != nil {
      Usage()
      return
    }
    value3 := argvalue3
    fmt.Print(client.PartitionJsonFile4b(context.Background(), value0, value1, value2, value3))
    fmt.Print("\n")
    break
  case "saveAsObjectFile":
    if flag.NArg() - 1 != 3 {
      fmt.Fprintln(os.Stderr, "SaveAsObjectFile requires 3 args")
      flag.Usage()
    }
    argvalue0 := flag.Arg(1)
    value0 := argvalue0
    tmp1, err87 := (strconv.Atoi(flag.Arg(2)))
    if err87 != nil {
      Usage()
      return
    }
    argvalue1 := int8(tmp1)
    value1 := argvalue1
    argvalue2, err88 := (strconv.ParseInt(flag.Arg(3), 10, 64))
    if err88 != nil {
      Usage()
      return
    }
    value2 := argvalue2
    fmt.Print(client.SaveAsObjectFile(context.Background(), value0, value1, value2))
    fmt.Print("\n")
    break
  case "saveAsTextFile":
    if flag.NArg() - 1 != 2 {
      fmt.Fprintln(os.Stderr, "SaveAsTextFile requires 2 args")
      flag.Usage()
    }
    argvalue0 := flag.Arg(1)
    value0 := argvalue0
    argvalue1, err90 := (strconv.ParseInt(flag.Arg(2), 10, 64))
    if err90 != nil {
      Usage()
      return
    }
    value1 := argvalue1
    fmt.Print(client.SaveAsTextFile(context.Background(), value0, value1))
    fmt.Print("\n")
    break
  case "saveAsJsonFile":
    if flag.NArg() - 1 != 3 {
      fmt.Fprintln(os.Stderr, "SaveAsJsonFile requires 3 args")
      flag.Usage()
    }
    argvalue0 := flag.Arg(1)
    value0 := argvalue0
    argvalue1, err92 := (strconv.ParseInt(flag.Arg(2), 10, 64))
    if err92 != nil {
      Usage()
      return
    }
    value1 := argvalue1
    argvalue2 := flag.Arg(3) == "true"
    value2 := argvalue2
    fmt.Print(client.SaveAsJsonFile(context.Background(), value0, value1, value2))
    fmt.Print("\n")
    break
  case "":
    Usage()
    break
  default:
    fmt.Fprintln(os.Stderr, "Invalid function ", cmd)
  }
}