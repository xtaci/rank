FROM golang:1.5
MAINTAINER xtaci <daniel820313@gmail.com>
ENV GOBIN /go/bin
COPY . /go
WORKDIR /go
ENV GOPATH /go:/go/.godeps
RUN go install rank
RUN rm -rf pkg src .godeps
ENTRYPOINT /go/bin/rank
RUN mkdir /data
VOLUME /data
EXPOSE 50001
