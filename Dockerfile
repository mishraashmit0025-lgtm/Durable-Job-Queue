FROM golang:1.27 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /jobq ./cmd/jobq

FROM gcr.io/distroless/static-debian12
COPY --from=build /jobq /jobq
VOLUME /data
EXPOSE 8080
ENTRYPOINT ["/jobq", "serve", "-addr", ":8080", "-db", "/data/jobq.db"]
