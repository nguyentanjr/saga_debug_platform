FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod go.sum main.go ./
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /step .

FROM scratch
COPY --from=build /step /step
ENTRYPOINT ["/step"]
