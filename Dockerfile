FROM golang:1.16-alpine
WORKDIR /go/src/arpcnet
COPY . .
RUN go get -d -v ./...
RUN go install -v ./...

EXPOSE 8028/TCP
EXPOSE 8029/TCP

ENTRYPOINT ["arpcnet"]
