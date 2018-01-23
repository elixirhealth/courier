#!/usr/bin/env bash

set -eou pipefail
#set -x  # useful for debugging

docker_cleanup() {
    echo "cleaning up existing network and containers..."
    docker ps | grep -E 'libri|courier' | awk '{print $1}' | xargs -I {} docker stop {} || true
    docker ps -a | grep -E 'libri|courier' | awk '{print $1}' | xargs -I {} docker rm {} || true
    docker network list | grep 'courier' | awk '{print $2}' | xargs -I {} docker network rm {} || true
}

# optional settings (generally defaults should be fine, but sometimes useful for debugging)
LIBRI_LOG_LEVEL="${LIBRI_LOG_LEVEL:-INFO}"  # or DEBUG
LIBRI_TIMEOUT="${LIBRI_TIMEOUT:-5}"  # 10, or 20 for really sketchy network
COURIER_LOG_LEVEL="${COURIER_LOG_LEVEL:-INFO}"  # or DEBUG
COURIER_TIMEOUT="${COURIER_TIMEOUT:-5}"  # 10, or 20 for really sketchy network

# local and filesystem constants
LOCAL_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# container command constants
LIBRI_IMAGE="daedalus2718/libri:latest"  # latest release
N_LIBRARIANS=4
COURIER_IMAGE="gcr.io/elxir-core-infra/service-base-build:snapshot" # develop
N_COURIERS=3

echo
echo "cleaning up from previous runs..."
docker_cleanup

echo
echo "creating courier docker network..."
docker network create courier

echo
echo "starting librarians..."
librarian_addrs=""
librarian_containers=""
for c in $(seq 0 $((${N_LIBRARIANS} - 1))); do
    port=$((20100+c))
    metricsPort=$((20200+c))
    name="librarian-${c}"
    docker run --name "${name}" --net=courier -d -p ${port}:${port} ${LIBRI_IMAGE} \
        librarian start \
        --nSubscriptions 2 \
        --logLevel "${LIBRI_LOG_LEVEL}" \
        --publicPort ${port} \
        --publicHost ${name} \
        --localPort ${port} \
        --localMetricsPort ${metricsPort} \
        --bootstraps "librarian-0:20100"
    librarian_addrs="${name}:${port},${librarian_addrs}"
    librarian_containers="${name} ${librarian_containers}"
done
librarian_addrs=${librarian_addrs::-1}  # remove trailing space
sleep 5

echo
echo "testing librarians health..."
docker run --rm --net=courier ${LIBRI_IMAGE} test health \
    -a "${librarian_addrs}" \
    --logLevel "${LIBRI_LOG_LEVEL}" \
    --timeout "${LIBRI_TIMEOUT}"


# start couriers

# test health

# test io

echo
echo "cleaning up..."
docker_cleanup

echo
echo "All tests passed."
