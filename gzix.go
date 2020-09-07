package main

import (
    "bufio"
    "compress/gzip"
    "fmt"
    "io"
    "io/ioutil"
    "log"
    "os"
    "strconv"
    "strings"
)

func main() {
    dir := parse_args()
    files, err := readDir(dir)
    if err != nil {
        log.Fatal(err)
    }

    // Open gzip file.
    zf, _ := os.OpenFile(dir+".gz", os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
    if err != nil {
        log.Fatal(err)
    }
    defer zf.Close()

    // Open index file.
    index, err := os.OpenFile(dir+".idx", os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
    if err != nil {
        log.Fatal(err)
    }
    defer index.Close()

    for _, f := range files {
        if f.IsDir() {
            continue
        }
        fpath := dir + "/" + f.Name()
        value := gzAdd(zf, fpath)
        index.WriteString(f.Name() + value)
    }

}

func parse_args() string {
    if len(os.Args) < 2 {
        log.Fatal(`indexing usage: gzix <folder>
                    get usage     : gzix -g  <file_name>`)
    }
    if os.Args[1] == "-g" {
        gzGet(os.Args[2], os.Args[3], os.Args[4])
        log.Fatal("done getting...")
    }
    folder := os.Args[1]
    return folder
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
// |<offset>|<size>
func gzAdd(zf *os.File, fpath string) string {
    f, _ := os.Open(fpath)
    defer f.Close()

    info, err := zf.Stat()
    if err != nil {
        log.Fatal(err)
    }
    offset := info.Size()

    b, err := ioutil.ReadAll(f)
    if err != nil {
        log.Fatal(err)
    }

    zw := gzip.NewWriter(zf)
    zw.Write(b)
    if err := zw.Close(); err != nil {
        log.Println(err)
    }
    zw.Reset(zf)

    ninfo, err := zf.Stat()
    if err != nil {
        log.Fatal(err)
    }

    size := ninfo.Size() - offset
    value := fmt.Sprintf(",%v,%v\n", offset, size)
    return value
}

func gzGet(index, gz, fname string) {
    // Open index file
    idx, err := os.Open(index)
    if err != nil {
        log.Fatal(err)
    }
    defer idx.Close()

    offset, length := meta(idx, fname)
    fmt.Println(offset, length)

    // Open database file
    f, err := os.Open(gz)
    if err != nil {
        log.Fatal(err)
    }
    defer f.Close()

    process(chunk(f, int64(offset), int64(length)))
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

func chunk(file *os.File, offset, byteLength int64) *gzip.Reader {

    ret, err := file.Seek(offset, os.SEEK_SET) // Byte offset after file start
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("Seek to byte %d …\n", ret)

    r, err := gzip.NewReader(io.NewSectionReader(file, offset, byteLength))
    if err != nil {
        log.Fatal(err)
    }
    return r
}

func process(r io.Reader) {
    b, err := ioutil.ReadAll(r)
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("Decompressed %d bytes, Decoded: %#v\n", len(b), string(b))
}
