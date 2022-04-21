# Airflow capstone project

## How to run this project:
- `git clone https://github.com/gridu/AIRFLOW-Dmytro-Shurko`
- Allocate enough memory for the Docker (e.g. 4 GB). (Settings → Resources → Memory)
- `docker-compose up --build`
- Start the Docker container (if it is not running)
    - `docker-compose up` (terminal) or
    - "Start" button (Docker Desktop)
- Open Airflow UI (http://localhost:8080/)
	- **login**: admin
	- **password**: admin
- Add connections (Admin → Connections)
	- **Conn Id**: fs_default, **Conn Type**: File (path), **Extra**: {"path": "/opt/airflow/files"}
	- **Conn Id**: postgres_default, **Conn Type**: Postgres, **Host**: postgres, **Login**: airflow, **Password**: airflow, **Port**: 5432
- Add the name of the run file. (Optional, default file_name is "run")
	- Admin → Variables → Add a new record
	- **Key**: name_path_variable, **Val**: &lt;file_name&gt;
- Add Slack token to Vault
    - `docker exec -it <VAULT_DOCKER_ID> sh`
    - `vault login ZyrP7NtNw0hbLUqu7N3IlTdO`
    - `vault secrets enable -path=airflow -version=2 kv`
    - `vault kv put airflow/variables/slack_token value=xoxb-2695075024663-2722400695905-QFWmCElinlHfJatEBV8IvhyI`
