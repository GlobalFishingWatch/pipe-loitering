FROM gcr.io/world-fishing-827/github.com/globalfishingwatch/gfw-pipeline:latest-python3.8

# Setup local application dependencies
COPY . ${WORKDIR}
RUN pip install -r requirements-scheduler.txt

# Setup local module
RUN pip install -e .
