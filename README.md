# Loitering pipeline

## Introduction

This repository contains the loitering pipeline, a dataflow pipeline that computes vessel loitering events.

## Development

### Dependencies

You just need [docker](https://www.docker.com/) and [docker-compose](https://docs.docker.com/compose/) in your machine to run the pipeline. No other dependency is required.

### Setup

The pipeline connects to various services from Google Cloud, so you need to first authenticate with your google cloud account inside the docker images. To do that, you need to run this command and follow the instructions:

```
docker-compose run pipeline gcloud auth application-default login
```

You also need to setup your Google Cloud default project to bill for those services via this command:

```
docker-compose run pipeline gcloud config set project [YOUR PROJECT]
```

### Tests

There are [pytest](https://docs.pytest.org/) tests in the `tests` directory, which can be run via `docker-compose run pipeline pytest`.

## License

Copyright 2021 Global Fishing Watch

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
