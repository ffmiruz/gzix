package main

import (
    "compress/gzip"
    "fmt"
    "io/ioutil"
    "log"
    "os"
)

func main() {
    dir := parse_args()
    files, err := readDir(dir)
    if err != nil {
        log.Fatal(err)
    }

    zf, _ := os.OpenFile(dir+".gz", os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
    if err != nil {
        log.Fatal(err)
    }
    defer zf.Close()

    index, err := os.OpenFile(dir+".idx", os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
    if err != nil {
        log.Fatal(err)
    }
    defer index.Close()

    for _, f := range files {
        fpath := dir + "/" + f.Name()
        value := gzAdd(zf, fpath)
        index.WriteString(f.Name() + value)
    }

}

func parse_args() string {
    if len(os.Args) < 2 {
        log.Fatal("usage: gzix <folder_location>")
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
    value := fmt.Sprintf("|%v|%v\n", offset, size)
    return value
}
