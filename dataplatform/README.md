# Data Platform

This package is meant to store utility code that can be shared between various flows used to build and maintain a data platform on top of Prefect

The code shown here uses Snowflake, dbt, AWS, and Slack for notifications but you can easily:
1. Swap components: 
   - replace Snowflake with BigQuery or Redshift
   - replace S3 with GCS, or Azure
   - replace SlackWebhook for other notification block e.g. TeamsWebhook
2. Extend the functionality with:
   - commonly used functions and classes
   - shared utilities
   - business logic code

You can install the package from the root project directory:
```
pip install .
```

Then, you can create your blocks using the code as shown in the `create_blocks` directory.
