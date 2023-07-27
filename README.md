# Superdao airflow

Repository contains airflow pipelines (DAGs) that derive blockchain data from dedicated node
via Ethereum ETL to Postgres database with quite plot structure.

### Required airflow connections:
clickhouse-eth-data - connection to ClickHouse for raw data and DBT models
google-cloud - connection to Google Cloud Storage (for transfer datasets between databases)
pg-prod-scoring-api - connection to Postgres for showcase API is hosted
slack_notifications - connection to Slack (for notifications)

### Project structure

- /dags - the main package containing definitions of our airflow dags
- /common - common modules
- /deployments/local - contains docker-compose file for local debugging 
- /dbt_clickhouse - contains DBT models

### Directory Descriptions
- _analytics_: Contains DAGs related to analytics tasks, such as generating reports, data analysis, or running machine learning models.
- _api_: Includes DAGs related to API integration tasks, such as fetching and processing data from external APIs.
- _attributes_: Contains DAGs focused on extracting and processing attribute-related data, such as wallet attributes or labels.
- _audiences_: Holds DAGs related to audience-related tasks, such as creating and updating audience segments.
- _chains/ethereum_etl_: Contains DAGs specifically related to Ethereum ETL tasks, handling the extraction, transformation, and loading of Ethereum blockchain data.
- _control_: Contains DAGs that serve as control mechanisms or orchestrators for other DAGs.
- _dbt_: Holds DAGs related to DBT (Data Build Tool) tasks, responsible for transforming and modeling the extracted data.
- _deanonimization_: Includes DAGs related to de-anonymization tasks, linking anonymized data to specific individuals or entities.
- _ens_: Contains DAGs related to Ethereum Name Service (ENS) tasks, including data extraction, processing, and storage.
- _erc_1155_: Holds DAGs specifically related to ERC-1155 token tasks, including data extraction, processing, and storage.
- _external_: Includes DAGs that interact with external systems or services outside of the Airflow environment.
- _ml_: Contains DAGs related to machine learning tasks, such as training models and running data pipelines.
- _nft_holders_: Holds DAGs specifically related to NFT (Non-Fungible Token) holder tasks, including data extraction, processing, and storage.
- _report_: Contains DAGs related to generating and delivering reports based on the extracted data.
- _snapshot_: Holds DAGs related to snapshotting or creating snapshots of the blockchain data at specific points in time.
- _tests_: Includes DAGs specifically designed for testing purposes, such as testing data pipelines or validating data quality.
- _top_collections_: Contains DAGs related to tasks involving top collections or popular items within a collection.
- _utility_: Holds utility or helper DAGs providing common functions or tasks used across other DAGs.
- _monodag.py_: Represents a single DAG that encapsulates a specific workflow or task.

### Pipeline Details
**The airflow DAGs in this repository follow the following pipeline:**

- Load raw data: The DAG uses the open-source tool named Ethereum ETL (https://github.com/blockchain-etl/ethereum-etl) to download raw data from the Ethereum blockchain.

- Insert into ClickHouse: The downloaded raw data is then inserted into ClickHouse, a columnar database optimized for analytics.

- Business Analytics with DBT: The data stored in ClickHouse is further processed using DBT (https://github.com/dbt-labs/dbt-core) to calculate several business analytics entities. DBT provides a powerful toolkit for transforming and modeling data.

- API Integration: The resulting tables or models from DBT are then utilized in an API, allowing users to access the derived analytics data.


### Debug on localhost

`make run-local` - running local airflow instance with [CeleryExecutor](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/executor/celery.html) and all production connections (postgres, clickhouse, google-cloud)

`make down` - stopping a local airflow instance and deleting all containers and volumes

`make delete` - the same as `make down` above, only with removal all images


