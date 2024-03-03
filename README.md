# Data Pipeline

This project is a simple data pipeline implemented in Python that utilizes Kafka and Elasticsearch for data streaming and storage. The pipeline is designed to handle data from a Kafka topic and index it in Elasticsearch, allowing for easy querying and analysis using Kibana.

## Setup

### Prerequisites

Before running the program, make sure you have the following services installed and running:

- **Kafka:**
  ```bash
  ./bin/kafka-server-start.sh ./config/kraft/server.properties
  ```

- **Elasticsearch:**
  ```bash
  ./bin/elasticsearch
  ```

- **Kibana:**
  ```bash
  ./bin/kibana
  ```

### Running the Program

Once the services are up and running, you can start the data pipeline:

```bash
python main.py
```

### Accessing Kibana

To query and visualize the data, access Kibana using the following link:

[http://localhost:5601/app/dev_tools#/console](http://localhost:5601/app/dev_tools#/console)

## Sample Query

Here are some sample queries that you can use for analyzing the data:

### Query 1

```json
GET /firsttopic/_search
{
  "query": {
    "match_all": {}
  }
}
```

### Query 2

```json
POST /firsttopic/_search
{
  "query": {
    "match": {
      "state": "North Carolina"
    }
  }
}
```

### Query 3

```json
POST /firsttopic/_search
{
  "query": {
    "term": {
      "state.keyword": "North Carolina"
    }
  }
}
```

Feel free to customize and expand these queries based on your data and analysis requirements.
