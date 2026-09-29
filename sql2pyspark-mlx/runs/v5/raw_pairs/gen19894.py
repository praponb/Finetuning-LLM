from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((~F.col("plan").isin(["premium", "standard"]) & F.col("start_date").between("2023-06-23", "2024-12-01")) | (F.col("plan") == "premium"))
)
