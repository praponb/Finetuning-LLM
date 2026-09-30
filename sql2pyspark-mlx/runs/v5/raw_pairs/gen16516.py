from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("opened_date") > "2024-05-01") & F.col("status").isin(["CLOSED", "OPEN", "FROZEN"]))
    .groupBy("status")
    .agg(F.countDistinct("balance").alias("unique_statuss"))
)
