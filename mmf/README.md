
# go-mmstruct / __mmf__ [![GoDoc](https://godoc.org/github.com/HellButcher/go-mmstruct/mmf?status.svg)](https://godoc.org/github.com/HellButcher/go-mmstruct/mmf)
working with _memory mapped files_ in golang.

## Installation

Go and get it...
```
$ go get github.com/HellButcher/go-mmstruct/mmf
```

## Usage

Creating a file:
```go
package main

import (
  "github.com/HellButcher/go-mmstruct/mmf"
)

func main() {
  mf, err := mmf.CreateMappedFile("aNewFile.bin", 4096)
  if err != nil {
    // ...
  }
  defer mf.Close()
  // do something in the mapped memory area
  data := mf.Bytes()
  data[1337] = 42  // writing
  foo := data[666] // reading
}

```

Opening a file:
```go
package main

import (
  "github.com/HellButcher/go-mmstruct/mmf"
)

func main() {
  mf, err := mmf.OpenMappedFile("anExistingFile.bin")
  if err != nil {
    // ...
  }
  defer mf.Close()
  // do something in the mapped memory area
  data := mf.Bytes()
  data[666] = 42    // writing
  foo := data[1337] // reading
}

```

The [`MappedFile`](https://godoc.org/github.com/HellButcher/go-mmstruct/mmf#MappedFile)
object can also be used as a [`Reader`](https://godoc.org/io#Reader) or
[`Writer`](https://godoc.org/io#Writer) (and some other interfaces).
