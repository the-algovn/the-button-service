FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o /out/the-button-service ./cmd/the-button-service \
 && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o /out/the-button-worker ./cmd/the-button-worker \
 && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o /out/the-button-migrate ./cmd/the-button-migrate

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/the-button-service /the-button-service
COPY --from=build /out/the-button-worker /the-button-worker
COPY --from=build /out/the-button-migrate /the-button-migrate
ENTRYPOINT ["/the-button-service"]
