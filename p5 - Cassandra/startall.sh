#!/usr/bin/env bash
echo "composing docker cluster..."
docker compose up -d
sleep 90
echo "done"
echo "launching jupyterlab..."
JUPYTER=$(docker ps | grep "127.0.0.1:5000->5000/tcp")
docker exec -it -d ${JUPYTER::12} python3 -m jupyterlab --no-browser --ip=0.0.0.0 --port=5000 --allow-root --NotebookApp.token=''
echo "done"
PORT=$((5001 + $RANDOM % 2))
SERVER=$(docker ps | grep "127.0.0.1:${PORT}->5000/tcp")
echo "starting server.py on port ${PORT} container_id ${SERVER::12}..."
echo ${SERVER::12} > "nb/server_container_id.txt"
docker exec -it ${SERVER::12} python3 /share/server.py
echo "done"
