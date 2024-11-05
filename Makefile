PROJECT_NAME=etl-invoice-payment-prediction
VERSION=1.0

build:
	@docker build -t cluster-apache-spark:3.5.3 .

run-app:
	@docker-compose up -d

build-test:
	@docker build -f Dockerfile.test -t $(PROJECT_NAME):latest .

test: build-test
	@docker run -it --rm $(PROJECT_NAME):latest

log:
	@docker logs etl-invoice-payment-prediction-spark-submit-client-1	