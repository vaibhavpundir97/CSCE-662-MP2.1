trap ctrl_c INT

function ctrl_c() {
    kill -9 $server1_pid
    echo ""
}

./tsd -c 1 -s 1 -h localhost -k 9000 -p 10000 &
server1_pid=$!
sleep 0.25
echo ""
sleep 0.5

./tsd -c 1 -s 2 -h localhost -k 9000 -p 10001

