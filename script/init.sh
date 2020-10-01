DIR="/Users/zzh/Desktop/cockroach-data"
cockroach start-single-node --insecure --listen-addr=localhost:26257 --http-addr=localhost:8080 --store $DIR --background
cockroach sql --insecure --host=localhost:26257 < ../src/init.sql