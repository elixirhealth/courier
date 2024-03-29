#!/usr/bin/env bash

set -eou pipefail
#set -x  # useful for debugging

docker_cleanup() {
    echo "cleaning up existing network and containers..."
    CONTAINERS='libri|courier|catalog'
    docker ps | grep -E ${CONTAINERS} | awk '{print $1}' | xargs -I {} docker stop {} || true
    docker ps -a | grep -E ${CONTAINERS} | awk '{print $1}' | xargs -I {} docker rm {} || true
    docker network list | grep 'courier' | awk '{print $2}' | xargs -I {} docker network rm {} || true
}

# optional settings (generally defaults should be fine, but sometimes useful for debugging)
LIBRI_LOG_LEVEL="${LIBRI_LOG_LEVEL:-INFO}"  # or DEBUG
LIBRI_TIMEOUT="${LIBRI_TIMEOUT:-5}"  # 10, or 20 for really sketchy network
CATALOG_LOG_LEVEL="${CATALOG_LOG_LEVEL:-INFO}"  # or DEBUG
CATALOG_TIMEOUT="${CATALOG_TIMEOUT:-5}"  # 10, or 20 for really sketchy network
KEY_LOG_LEVEL="${CATALOG_LOG_LEVEL:-INFO}"  # or DEBUG
KEY_TIMEOUT="${CATALOG_TIMEOUT:-5}"  # 10, or 20 for really sketchy network
COURIER_LOG_LEVEL="${COURIER_LOG_LEVEL:-INFO}"  # or DEBUG
COURIER_TIMEOUT="${COURIER_TIMEOUT:-5}"  # 10, or 20 for really sketchy network
COURIER_TEST_IO_N_DOCS="${COURIER_TEST_IO_N_DOCS:-8}"

# local and filesystem constants
LOCAL_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# container command constants
LIBRI_IMAGE="daedalus2718/libri:latest"  # latest release
N_LIBRARIANS=4
COURIER_IMAGE="gcr.io/elixir-core-prod/courier:snapshot" # develop
CATALOG_IMAGE="gcr.io/elixir-core-prod/catalog:snapshot" # develop
KEY_IMAGE="gcr.io/elixir-core-prod/key:snapshot" # develop

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
echo "testing librarian health..."
docker run --rm --net=courier ${LIBRI_IMAGE} test health \
    -a "${librarian_addrs}" \
    --logLevel "${LIBRI_LOG_LEVEL}" \
    --timeout "${LIBRI_TIMEOUT}"

echo
echo "starting catalog..."
port=10100
name="catalog-0"
docker run --name "${name}" --net=courier -d -p ${port}:${port} ${CATALOG_IMAGE} \
    start \
    --logLevel "${CATALOG_LOG_LEVEL}" \
    --serverPort ${port} \
    --storageMemory
catalog_addr="${name}:${port}"
catalog_containers="${name}"

echo
echo "testing catalog health..."
docker run --rm --net=courier ${CATALOG_IMAGE} test health \
    --catalogs "${catalog_addr}" \
    --logLevel "${CATALOG_LOG_LEVEL}"

echo
echo "starting key..."
port=10200
name="key-0"
docker run --name "${name}" --net=courier -d -p ${port}:${port} ${KEY_IMAGE} \
    start \
    --logLevel "${KEY_LOG_LEVEL}" \
    --serverPort ${port} \
    --storageMemory
key_addr="${name}:${port}"
key_containers="${name}"

echo
echo "testing key health..."
docker run --rm --net=courier ${KEY_IMAGE} test health \
    --addresses "${key_addr}" \
    --logLevel "${KEY_LOG_LEVEL}"

echo
echo "starting courier..."
port=10300
name="courier-0"
docker run --name "${name}" --net=courier -d -p ${port}:${port} ${COURIER_IMAGE} \
    start \
    --logLevel "${LIBRI_LOG_LEVEL}" \
    --serverPort ${port} \
    --librarians ${librarian_addrs} \
    --key ${key_addr} \
    --catalog ${catalog_addr} \
    --storageMemory
courier_addrs="${name}:${port}"
courier_containers="${name}"

echo
echo "testing courier health..."
docker run --rm --net=courier ${COURIER_IMAGE} test health \
    --couriers "${courier_addrs}" \
    --logLevel "${LIBRI_LOG_LEVEL}"

echo
echo "testing courier input/output..."
docker run --rm --net=courier ${COURIER_IMAGE} test io \
    --couriers "${courier_addrs}" \
    --nDocs "${COURIER_TEST_IO_N_DOCS}" \
    --logLevel "${COURIER_LOG_LEVEL}" \
    --timeout "${COURIER_TIMEOUT}"

echo
echo "cleaning up..."
docker_cleanup

echo
echo "All tests passed."
