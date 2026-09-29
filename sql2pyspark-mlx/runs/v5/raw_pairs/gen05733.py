from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("opened_date") < "2024-08-01") & F.col("balance").between(21000, 31000))
)
