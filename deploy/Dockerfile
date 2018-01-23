FROM alpine:3.7

RUN apk update && \
    apk add bash bash-completion util-linux coreutils findutils grep

ADD bin/* /usr/local/bin/

ENTRYPOINT ["courier"]