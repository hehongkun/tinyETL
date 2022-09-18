FROM golang:1.17.2

RUN mkdir -p /go/src/tiny_etl/static/

WORKDIR /go/src/tiny_etl/

COPY . /go/src/tiny_etl/

ENV GO111MODULE=on
ENV GOROOT=/usr/local/go
ENV GOPATH=/home/gopath
ENV PATH=$PATH:$GOROOT/bin:$GOPATH/bin
ENV GOPROXY=https://goproxy.io

EXPOSE 8000

RUN chmod 777 ./main

ENTRYPOINT ["./main"]