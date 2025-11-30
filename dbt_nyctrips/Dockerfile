FROM python:3.12-slim

# Set the environment variable to increase the PIP timeout to 100 seconds
ENV PIP_DEFAULT_TIMEOUT=100

# Install dbt
RUN pip install --no-cache-dir \
    dbt-core==1.10.13 \
    dbt-bigquery==1.10.3

RUN apt-get update && apt-get install -y git

WORKDIR /usr/app/dbt

COPY . .

# Install dependencies
RUN dbt deps

USER root

CMD ["tail", "-f", "/dev/null"]
