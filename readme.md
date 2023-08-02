# Fetch Rewards
## Data Engineering Take Home: ETL from an SQS Queue

This is a Python-based ETL script that pulls messages from an AWS SQS queue, performs data transformations (masking IP and device IDs, converting app versions), and loads the data into the `user_logins` table in PostgreSQL.

### Prerequisites

Before running this script, make sure you have the following installed:

- Python 3 or the latest version
- Docker with Docker Compose support

### Installation

1. Clone this repository to your local machine.
2. Install the required Python dependencies:

   - `boto3` for interacting with AWS SQS
   - `pandas` for data manipulation
   - `psycopg2` for connecting to PostgreSQL

### Configuration

Before running the script, ensure that you have configured the following files:

1. `config/config.py`: Modify this file to set the appropriate values for `host` and `queue_url`.

2. `security/security_postgres.py`: Update this file with the proper PostgreSQL credentials.

3. `docker-compose.yml`: Verify the images used for Localstack and Postgres.

### How to Run

1. Open a terminal/command prompt.
2. Navigate to the `fetch_hometake` folder.
3. Run the following commands in the terminal/command prompt:

   - `docker-compose up -d` to start the Localstack and Postgres containers with the required test data.

   - `python etl_script.py` to start the ETL process.

4. Monitor the logs in the `logs/aws_sqs_queue.log` file for any messages or errors during the execution.

If you encounter any issues or have questions, feel free to reach out to abduvakhid@gmail.com 
