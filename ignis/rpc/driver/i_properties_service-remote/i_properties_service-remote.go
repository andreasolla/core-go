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
	"ignis/rpc/driver"
)

var _ = driver.GoUnusedProtection__

func Usage() {
  fmt.Fprintln(os.Stderr, "Usage of ", os.Args[0], " [-h host:port] [-u url] [-f[ramed]] function [arg1 [arg2...]]:")
  flag.PrintDefaults()
  fmt.Fprintln(os.Stderr, "\nFunctions:")
  fmt.Fprintln(os.Stderr, "  i64 newInstance()")
  fmt.Fprintln(os.Stderr, "  i64 newInstance2(i64 id)")
  fmt.Fprintln(os.Stderr, "  string setProperty(i64 id, string key, string value)")
  fmt.Fprintln(os.Stderr, "  string getProperty(i64 id, string key)")
  fmt.Fprintln(os.Stderr, "  string rmProperty(i64 id, string key)")
  fmt.Fprintln(os.Stderr, "  bool contains(i64 id, string key)")
  fmt.Fprintln(os.Stderr, "   toMap(i64 id, bool defaults)")
  fmt.Fprintln(os.Stderr, "  void fromMap(i64 id,  _map)")
  fmt.Fprintln(os.Stderr, "  void load(i64 id, string path)")
  fmt.Fprintln(os.Stderr, "  void store(i64 id, string path)")
  fmt.Fprintln(os.Stderr, "  void clear(i64 id)")
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
  client := driver.NewIPropertiesServiceClient(thrift.NewTStandardClient(iprot, oprot))
  if err := trans.Open(); err != nil {
    fmt.Fprintln(os.Stderr, "Error opening socket to ", host, ":", port, " ", err)
    os.Exit(1)
  }
  
  switch cmd {
  case "newInstance":
    if flag.NArg() - 1 != 0 {
      fmt.Fprintln(os.Stderr, "NewInstance_ requires 0 args")
      flag.Usage()
    }
    fmt.Print(client.NewInstance_(context.Background()))
    fmt.Print("\n")
    break
  case "newInstance2":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "NewInstance2_ requires 1 args")
      flag.Usage()
    }
    argvalue0, err39 := (strconv.ParseInt(flag.Arg(1), 10, 64))
    if err39 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.NewInstance2_(context.Background(), value0))
    fmt.Print("\n")
    break
  case "setProperty":
    if flag.NArg() - 1 != 3 {
      fmt.Fprintln(os.Stderr, "SetProperty requires 3 args")
      flag.Usage()
    }
    argvalue0, err40 := (strconv.ParseInt(flag.Arg(1), 10, 64))
    if err40 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    argvalue1 := flag.Arg(2)
    value1 := argvalue1
    argvalue2 := flag.Arg(3)
    value2 := argvalue2
    fmt.Print(client.SetProperty(context.Background(), value0, value1, value2))
    fmt.Print("\n")
    break
  case "getProperty":
    if flag.NArg() - 1 != 2 {
      fmt.Fprintln(os.Stderr, "GetProperty requires 2 args")
      flag.Usage()
    }
    argvalue0, err43 := (strconv.ParseInt(flag.Arg(1), 10, 64))
    if err43 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    argvalue1 := flag.Arg(2)
    value1 := argvalue1
    fmt.Print(client.GetProperty(context.Background(), value0, value1))
    fmt.Print("\n")
    break
  case "rmProperty":
    if flag.NArg() - 1 != 2 {
      fmt.Fprintln(os.Stderr, "RmProperty requires 2 args")
      flag.Usage()
    }
    argvalue0, err45 := (strconv.ParseInt(flag.Arg(1), 10, 64))
    if err45 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    argvalue1 := flag.Arg(2)
    value1 := argvalue1
    fmt.Print(client.RmProperty(context.Background(), value0, value1))
    fmt.Print("\n")
    break
  case "contains":
    if flag.NArg() - 1 != 2 {
      fmt.Fprintln(os.Stderr, "Contains requires 2 args")
      flag.Usage()
    }
    argvalue0, err47 := (strconv.ParseInt(flag.Arg(1), 10, 64))
    if err47 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    argvalue1 := flag.Arg(2)
    value1 := argvalue1
    fmt.Print(client.Contains(context.Background(), value0, value1))
    fmt.Print("\n")
    break
  case "toMap":
    if flag.NArg() - 1 != 2 {
      fmt.Fprintln(os.Stderr, "ToMap requires 2 args")
      flag.Usage()
    }
    argvalue0, err49 := (strconv.ParseInt(flag.Arg(1), 10, 64))
    if err49 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    argvalue1 := flag.Arg(2) == "true"
    value1 := argvalue1
    fmt.Print(client.ToMap(context.Background(), value0, value1))
    fmt.Print("\n")
    break
  case "fromMap":
    if flag.NArg() - 1 != 2 {
      fmt.Fprintln(os.Stderr, "FromMap requires 2 args")
      flag.Usage()
    }
    argvalue0, err51 := (strconv.ParseInt(flag.Arg(1), 10, 64))
    if err51 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    arg52 := flag.Arg(2)
    mbTrans53 := thrift.NewTMemoryBufferLen(len(arg52))
    defer mbTrans53.Close()
    _, err54 := mbTrans53.WriteString(arg52)
    if err54 != nil { 
      Usage()
      return
    }
    factory55 := thrift.NewTJSONProtocolFactory()
    jsProt56 := factory55.GetProtocol(mbTrans53)
    containerStruct1 := driver.NewIPropertiesServiceFromMapArgs()
    err57 := containerStruct1.ReadField2(context.Background(), jsProt56)
    if err57 != nil {
      Usage()
      return
    }
    argvalue1 := containerStruct1._map
    value1 := argvalue1
    fmt.Print(client.FromMap(context.Background(), value0, value1))
    fmt.Print("\n")
    break
  case "load":
    if flag.NArg() - 1 != 2 {
      fmt.Fprintln(os.Stderr, "Load requires 2 args")
      flag.Usage()
    }
    argvalue0, err58 := (strconv.ParseInt(flag.Arg(1), 10, 64))
    if err58 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    argvalue1 := flag.Arg(2)
    value1 := argvalue1
    fmt.Print(client.Load(context.Background(), value0, value1))
    fmt.Print("\n")
    break
  case "store":
    if flag.NArg() - 1 != 2 {
      fmt.Fprintln(os.Stderr, "Store requires 2 args")
      flag.Usage()
    }
    argvalue0, err60 := (strconv.ParseInt(flag.Arg(1), 10, 64))
    if err60 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    argvalue1 := flag.Arg(2)
    value1 := argvalue1
    fmt.Print(client.Store(context.Background(), value0, value1))
    fmt.Print("\n")
    break
  case "clear":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "Clear requires 1 args")
      flag.Usage()
    }
    argvalue0, err62 := (strconv.ParseInt(flag.Arg(1), 10, 64))
    if err62 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.Clear(context.Background(), value0))
    fmt.Print("\n")
    break
  case "":
    Usage()
    break
  default:
    fmt.Fprintln(os.Stderr, "Invalid function ", cmd)
  }
}
