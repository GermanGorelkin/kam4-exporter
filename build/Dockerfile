FROM golang:1.18-alpine as builder
ARG CMD
RUN apk update && apk add --no-cache git ca-certificates && update-ca-certificates

WORKDIR /app
COPY . ./
RUN CGO_ENABLED=0 go build -o bin/app -v cmd/${CMD}/main.go


FROM scratch
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /app/bin/app ./

CMD ["./app"]