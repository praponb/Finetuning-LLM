from pyspark.sql import functions as F

result = df_accounts.select("id", "balance", "opened_date")
