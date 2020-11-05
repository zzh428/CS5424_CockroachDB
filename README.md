# CockroachDB Performance
## Step1: Setup Environment

### CockroachDB
#### Download the Binary
```bash
$ wget -qO- https://binaries.cockroachdb.com/cockroach-v19.2.9.linux-amd64.tgz | tar  xvz
```
#### Add cockroach directory to PATH
```bash
$ export PATH=$PATH:${HOME_PATH}/cockroach-v19.2.9.linux-amd64
```
```bash
$ cockroach version
Build Tag:    v19.2.9
Build Time:   2020/06/29 22:02:23
Distribution: CCL
Platform:     linux amd64 (x86_64-unknown-linux-gnu)
Go Version:   go1.12.12
C Compiler:   gcc 6.3.0
Build SHA-1:  5930d185b895e7deae41833af8fcce49babd23a1
Build Type:   release
```

### Go
#### Download the Binary

```bash
$ wget https://golang.org/dl/go1.15.3.linux-amd64.tar.gz
$ tar -xzf go1.15.3.linux-amd64.tar.gz
```
#### Add Go directory to PATH
```bash
$ export PATH=$PATH:${HOME_PATH}/go/bin
$ go version
go version go1.15.3 linux/amd64
```
#### Get Dependency

```bash
$ cd ${path_to_project}
$ go mod init cockroach 
$ go mod tidy
```

## Step 2: Start CockroachDB
Run the following command on all 4/5 nodes:
```bash
$ cockroach start --insecure --advertise-addr=${host:port} --join=${host1:port1},${host2:port2}...${hostn:portn} --cache=.25 --max-sql-memory=.25 --background --store=${directory_to_store_node_files}
```
The storing directory for each node should be different.

Then run `cockroach init` on one of these nodes. You can check the status of CockroachDB using `cockroach node status --insecure`.

## Step 3: Initialize Database
### Preprocess Data Dile

```bash
$ cd ${path_to_project}/script
$ python orderline.py ${path_to_project_files}/data_files/
```
### Copy Data Files to the Extern Directory of Each Node

```bash
$ mkdir ${directory_to_store_node_files}/extern
$ cp -r ${path_to_project_files}/data_files/ ${directory_to_store_node_files}/extern/project-files/data-files/
```

### Load Data
Run the following script before each experiment:
```bash
$ cd ${path_to_project}/script
$ ./init_cluster.sh
```

## Step 4: Run Experiments

```bash
$ cd ${path_to_project}/cockroachdb
$ go build cmd/filedriver/filedriver.go
# get command help
$ ./filedriver --help
```

Run the following command on all 5 nodes:
```bash
$ ./filedriver --user=${user_of_db} --database=${db_name} --endpoints=${host1:port1},${host2:port2}...${hostn:portn} --server-num=${server_node_num(4 or 5)} --server-seq=${1~server_node_num} --txn-file-num=${20 or 40} --dir=${path_to_xact-files} --out-file(optional)
```

When `--user`  not set, user will by default set to `root`

When `--database` not set, user will by default set to `wholesale`

If `--out-file` is specified, the log of each client will be stored at `${path_to_xact-files}/${client_num}.out`


## Step 5: Collect Statistics

### Collect Database State

```bash
$ cd ${path_to_project}/cockroachdb
$ go build cmd/dbstate/dbstate.go
# get command help
$ ./dbstate --help
# example
$ ./dbstate --user=${user_of_db} --database=${db_name} --endpoint=${host:port} --dir=${output_dir} --exp-num={5..8}
```

### Collect Client Measurements
```bash
$ cd ${path_to_project}/script
$ python merge_csv.py ${path_to_xact-files} ${exp-num(5..8)}
```

Client measurements will be stored at `${path_to_xact-files}/clients-${exp_num}.csv`

## Run SQL Code in Stdin Mode

```bash
$ cd ${path_to_project}/cockroachdb
$ go build cmd/stdindriver/stdindriver.go
# get command help
$ ./stdindriver --help
# example
$ ./stdindriver --user=${user_of_db} --database=${db_name} --endpoints=${host1:port1},${host2:port2}...${hostn:portn}
```

When `--user`  not set, user will by default set to `root`

When `--database` not set, user will by default set to `wholesale`



