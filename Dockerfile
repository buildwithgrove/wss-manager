FROM golang:1.22.3-alpine3.19 AS builder
RUN apk add --no-cache git

ARG GITHUB_TOKEN

ENV GOPRIVATE="github.com/pokt-foundation/*"

ENV GITHUB_TOKEN=$GITHUB_TOKEN

RUN git config --global url."https://${GITHUB_TOKEN}:x-oauth-basic@github.com/".insteadOf "https://github.com/"

WORKDIR /go/src/github.com/pokt-foundation
COPY . /go/src/github.com/pokt-foundation/wss-manager

WORKDIR /go/src/github.com/pokt-foundation/wss-manager
RUN CGO_ENABLED=0 GOOS=linux go build -a -o bin ./main.go

FROM alpine:3.19
WORKDIR /app

ARG IMAGE_TAG
ENV IMAGE_TAG=${IMAGE_TAG}

COPY --from=builder /go/src/github.com/pokt-foundation/wss-manager/bin ./

ENTRYPOINT ["/app/bin"]
