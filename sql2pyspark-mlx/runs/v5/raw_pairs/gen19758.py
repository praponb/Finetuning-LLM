from pyspark.sql import functions as F

result = df_accounts.select("opened_date", "account_type")
