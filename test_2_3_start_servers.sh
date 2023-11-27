trap ctrl_c INT

function ctrl_c() {
    kill -9 $server1_pid
    kill -9 $server2_pid
    kill -9 $server3_pid
    kill -9 $server4_pid
    kill -9 $server5_pid
    kill -9 $server6_pid
    kill -9 $synch1_pid
    kill -9 $synch2_pid
    echo ""
}

./tsd -c 1 -s 1 -h localhost -k 9000 -p 10000 &
server1_pid=$!
sleep 0.5
echo "[Cluster:1::Server:1] PID $server1_pid";
echo ""

./tsd -c 1 -s 2 -h localhost -k 9000 -p 10001 &
server2_pid=$!
sleep 0.5
echo "[Cluster:1::Server:2] PID $server2_pid";
echo ""

./synchronizer -h localhost -k 9000 -p 9001 -i 1 &
synch1_pid=$!
sleep 0.5

./tsd -c 2 -s 1 -h localhost -k 9000 -p 20000 &
server3_pid=$!
sleep 0.5
echo "[Cluster:2::Server:1] PID $server3_pid";
echo ""

./tsd -c 2 -s 2 -h localhost -k 9000 -p 20001 &
server4_pid=$!
sleep 0.5
echo "[Cluster:2::Server:2] PID $server4_pid";
echo ""

./synchronizer -h localhost -k 9000 -p 9002 -i 2 &
synch2_pid=$!
sleep 0.5

./tsd -c 3 -s 1 -h localhost -k 9000 -p 30000 &
server5_pid=$!
sleep 0.5
echo "[Cluster:3::Server:1] PID $server5_pid";
echo ""

./tsd -c 3 -s 2 -h localhost -k 9000 -p 30001 &
server6_pid=$!
sleep 0.5
echo "[Cluster:3::Server:2] PID $server6_pid";
echo ""

./synchronizer -h localhost -k 9000 -p 9003 -i 3