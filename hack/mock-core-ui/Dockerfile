FROM golang:1.22 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY hack/mockcorelib ./hack/mockcorelib
COPY hack/mock-core-ui ./hack/mock-core-ui
RUN CGO_ENABLED=0 go build -o /mock-core-ui ./hack/mock-core-ui

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /mock-core-ui /mock-core-ui
EXPOSE 8090
USER nonroot:nonroot
ENTRYPOINT ["/mock-core-ui"]
