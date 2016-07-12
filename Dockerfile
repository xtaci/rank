FROM golang:latest
MAINTAINER xtaci <daniel820313@gmail.com>
ENV GOBIN /go/bin
COPY . /go
WORKDIR /go
RUN go install rank
RUN rm -rf pkg src
ENTRYPOINT /go/bin/rank
RUN mkdir /data
VOLUME /data
EXPOSE 50001
