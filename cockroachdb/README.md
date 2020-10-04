# CockroachDB Performance
## Environment
```bash
$ go version
go version go1.14.3 darwin/amd64
```

#### Get Dependency

```bash
$ cd ${path_to_project}
$ go mod tidy
```

## Running Mode

#### Stdin Mode

```bash
$ go build cmd/stdindriver/stdindriver.go
# get command help
$ ./stdindriver --help
# example
$ ./stdindriver --user=${user_of_db} --database=${db_name} --endpoints=${host1:port1},${host2:port2}...${hostn:portn}
```

When `--user`  not set, user will by default set to `root`

When `--database` not set, user will by default set to `wholesale`

#### Xact-files Mode

```bash
$ go build cmd/filedriver/filedriver.go
# get command help
$ ./filedriver --help
# example
$ ./filedriver --user=${user_of_db} --database=${db_name} --endpoints=${host1:port1},${host2:port2}...${hostn:portn} --num=${client_num} --dir=${path_to_xact-files}
```

When `--user`  not set, user will by default set to `root`

When `--database` not set, user will by default set to `wholesale`