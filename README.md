# END TO END ETL Pipeline w/ Python, Airflow, MongoDB, and Docker

This project is a production ready pipeline. The purpose of this pipeline is to convert raw data into production ready readable data for data analytics in a scalable manner. The pipeline extracts weekly dumps of Reddit posts, Transform into readable JSON data with ONLY the necessary data + metadata, which is then loaded into a local MongoDB instance hosted on a Docker container. 

## Table of Contents
- [Installation](#installation)
- [Usage](#usage)
- [Configuration](#configuration)
- [Testing](#testing)
- [Contributing](#contributing)
- [License](#license)

## Installation

- Git Clone this repository into your local system
- Run docker compose up -- build to run a build version of the pipeline


## Usage

- Open http://localhost:8080/admin/ to access Airflow
- Run the redditDag on Airflow 

BONUS - Access MongoDB instance on your local machine
- In terminal, run docker ps
- Double check for nwoai-mongo-1 and copy the container ID
- Run docker exec -it <CONTAINER_ID> mongo
- In Mongo: show dbs, use redditDB show collections
- Once redditPosts is found: Run db.redditPosts.find()

## Configuration

- Change Services Versions: You can adjust the versions of any dependencies in the docker-compose.yml file
- You can change the cadence and schedule in redditDag.py

## Testing

- Open test_etl.py
- Run pytest in terminal in active directory

## Contributing

- The repository is public: You can fork the repository to make any further changes

## License

Information about the project's license.