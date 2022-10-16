prefect profile create dev
prefect profile use dev
prefect config set PREFECT_LOGGING_LEVEL=DEBUG
prefect config set PREFECT_API_KEY=your_api_key
prefect cloud workspace set --workspace "your_account/dev"