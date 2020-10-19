if [ ! -n "$1" ] ;then
    DIR=$(pwd)/cockroach-data
else
    DIR=$1
fi
echo "Data dir:" $DIR
cockroach start-single-node --insecure --listen-addr=localhost:26257 --http-addr=localhost:8080 --store $DIR --background
cockroach sql --insecure --host=localhost:26257 < init.sql