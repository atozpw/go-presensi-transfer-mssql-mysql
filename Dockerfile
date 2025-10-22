FROM golang:alpine

RUN apk update && apk add --no-cache git nano tzdata

ENV TZ=Asia/Jakarta

WORKDIR /app

COPY . .

RUN go mod tidy

RUN go build -o binary

ENTRYPOINT ["/app/binary"]