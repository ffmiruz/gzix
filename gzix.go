package main

import (
    "bufio"
    "compress/gzip"
    "fmt"
    "github.com/pkg/errors"
    "io"
    "io/ioutil"
    "log"
    "os"
    "strconv"
    "strings"
)

func main() {
    if len(os.Args) == 2 {
        err := index(os.Args[1])
        if err != nil {
            log.Println(err)
        }
    } else if len(os.Args) == 5 && os.Args[1] == "-g" {
        gzGet(os.Args[2], os.Args[3], os.Args[4])
    } else {
        log.Fatal(`Usage:-
                    Index a folder: gzix <folder>
                    Get value     : gzix -g  <index_file> <gz_file> <file_name>`)
    }
}

// Create files index and database of a folder
func index(dir string) error {
    files, err := readDir(dir)
    if err != nil {
        return errors.Wrap(err, "Can't get list of files")
    }

    // Open gzip file.
    zf, _ := os.OpenFile(dir+".gz", os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
    if err != nil {
        return errors.Wrap(err, "Can't open/create gzip file")
    }
    defer zf.Close()

    // Open index file.
    index, err := os.OpenFile(dir+".idx", os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
    if err != nil {
        return errors.Wrap(err, "Can't open/create index file")
    }
    defer index.Close()

    for _, f := range files {
        if f.IsDir() {
            continue
        }
        fpath := dir + "/" + f.Name()
        value, err := gzAdd(zf, fpath)
        if err != nil {
            log.Printf("Can't add %v: %v", fpath, err)
            continue
        }
        index.WriteString(f.Name() + value)
    }
    return nil
}

func readDir(dirname string) ([]os.FileInfo, error) {
    f, err := os.Open(dirname)
    if err != nil {
        return nil, err
    }
    list, err := f.Readdir(-1)
    f.Close()
    if err != nil {
        return nil, err
    }
    return list, nil
}

// gzip file and concatenate into the main gzip.
// Return string of fotmatted offset start position and binary size
// <filename>,<offset>,<size>
func gzAdd(zf *os.File, fpath string) (string, error) {
    var value string
    f, _ := os.Open(fpath)
    defer f.Close()

    info, err := zf.Stat()
    if err != nil {
        return value, err
    }
    offset := info.Size()

    b, err := ioutil.ReadAll(f)
    if err != nil {
        return value, err
    }

    zw := gzip.NewWriter(zf)
    zw.Write(b)
    if err := zw.Close(); err != nil {
        log.Println(err)
    }
    zw.Reset(zf)

    ninfo, err := zf.Stat()
    if err != nil {
        return value, err
    }

    size := ninfo.Size() - offset
    value = fmt.Sprintf(",%v,%v\n", offset, size)
    return value, nil
}

func gzGet(index, gz, fname string) {
    // Open index file
    idx, err := os.Open(index)
    if err != nil {
        log.Fatal(err)
    }
    defer idx.Close()

    offset, length := meta(idx, fname)

    // Open database file
    f, err := os.Open(gz)
    if err != nil {
        log.Fatal(err)
    }
    defer f.Close()

    gunzip(frame(f, int64(offset), int64(length)))
}

func meta(file *os.File, fname string) (offset, length int) {
    var err error
    scanner := bufio.NewScanner(file)
    for scanner.Scan() {
        line := scanner.Text()
        if strings.HasPrefix(line, fname+",") {
            info := strings.Split(line, ",")
            offsetText := info[1]
            lengthText := info[2]
            offset, err = strconv.Atoi(offsetText)
            if err != nil {
                log.Fatal(err)
            }
            length, err = strconv.Atoi(lengthText)
            if err != nil {
                log.Fatal(err)
            }
            break
        }
    }
    if err := scanner.Err(); err != nil {
        log.Fatal(err)
    }
    return

}

func frame(file *os.File, offset, byteLength int64) *gzip.Reader {
    start, err := file.Seek(offset, os.SEEK_SET)
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("Start read at byte: %d", start)

    r, err := gzip.NewReader(io.NewSectionReader(file, offset, byteLength))
    if err != nil {
        log.Fatal(err)
    }
    return r
}

func gunzip(r io.Reader) {
    b, err := ioutil.ReadAll(r)
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("Decompressed %d bytes, Decoded: %#v\n", len(b), string(b))
}
