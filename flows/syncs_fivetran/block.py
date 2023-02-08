from dotenv import load_dotenv
import os
from prefect_fivetran import FivetranCredentials

load_dotenv()

fivetran_credentials = FivetranCredentials(
    api_key=os.environ.get("FIVETRAN_API_KEY"),
    api_secret=os.environ.get("FIVETRAN_API_SECRET_KEY"),
)
fivetran_credentials.save("default")
