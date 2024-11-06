# etl-invoice-payment-prediction

## Pre requisites
* Docker installed
* Docker compose  installed

## Run the app and test

### Build the image

```sh
make build
```

### Run the app

```sh
make run-app
```

### Build unit test

```sh
make build-test
```

### Run unit test

```sh
make test
```

### Print application log

```sh
make log
```

## Output
After a successful run of the etl job app, an folder named "output" will be generated at the folder path "app/output" with the feature data in a CSV file

## Unit Test
Currently, the unit test is only testing transformer logics. Can add more to test out logics in CsvReader as well.

## Any more to do?
* add lint
* add mypy type check
* add more unit test
* add bandit code security test
* add Spark UI for performance analysis
* add error handling such as exception catching
* add unit test coverage and improve unit test coverage
* add [Integration Test](https://getindata.com/blog/integration-tests-spark-applications-big-data/)
* add CI/CD pipelines