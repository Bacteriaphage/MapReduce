package main

import (
	"fmt"
	"mapreduce"
	"os"
   "strings"
   "strconv"
   "unicode"
)

//
// The map function is called once for each file of input. The first
// argument is the name of the input file, and the second is the
// file's complete contents. You should ignore the input file name,
// and look only at the contents argument. The return value is a slice
// of key/value pairs.
//
func split(r rune) bool{
   return !unicode.IsLetter(r) && !unicode.IsNumber(r)
}
func mapF(filename string, contents string) []mapreduce.KeyValue {
	// TODO: you have to write this function
   var raw_data []string
   raw_data = strings.FieldsFunc(contents, split)
   //fmt.Println(raw_data)
   var counter map[string]int
   counter = make(map[string]int)
   for _, temp := range raw_data{
      //temp = strings.ToLower(temp)
      _,err := counter[temp]
      if !err{
         counter[temp] = 1
      } else{
         counter[temp]++
      }
   }
   var result []mapreduce.KeyValue
   for key, value := range counter{
      result = append(result, mapreduce.KeyValue{key, strconv.Itoa(value)})
   }
   return result
}

//
// The reduce function is called once for each key generated by the
// map tasks, with a list of all the values created for that key by
// any map task.
//
func reduceF(key string, values []string) string {
	// TODO: you also have to write this function
   var sum int = 0
   for _, value := range values{
      temp, err := strconv.Atoi(value)
      if err != nil{
         panic(err)
      } else{
         sum += temp
      }
   }
   return strconv.Itoa(sum)
}

// Can be run in 3 ways:
// 1) Sequential (e.g., go run wc.go master sequential x1.txt .. xN.txt)
// 2) Master (e.g., go run wc.go master localhost:7777 x1.txt .. xN.txt)
// 3) Worker (e.g., go run wc.go worker localhost:7777 localhost:7778 &)
func main() {
	if len(os.Args) < 4 {
		fmt.Printf("%s: see usage comments in file\n", os.Args[0])
	} else if os.Args[1] == "master" {
		var mr *mapreduce.Master
		if os.Args[2] == "sequential" {
			mr = mapreduce.Sequential("wcseq", os.Args[3:], 3, mapF, reduceF)
		} else {
			mr = mapreduce.Distributed("wcseq", os.Args[3:], 3, os.Args[2])
		}
		mr.Wait()
	} else {
		mapreduce.RunWorker(os.Args[2], os.Args[3], mapF, reduceF, 100)
	}
}
