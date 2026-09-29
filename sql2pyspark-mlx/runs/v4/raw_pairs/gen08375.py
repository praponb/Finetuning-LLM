from pyspark.sql import functions as F

base = (
    df_accounts
    .filter(F.col("opened_date") >= F.lit("2023-05-07").cast("date"))
    .withColumn("status_filled", F.coalesce("status", F.lit("unassigned")))
)

result = (
    base
    .groupBy("customer_id")
    .pivot("account_type", ["CREDIT", "CHECKING"])
    .agg(F.max("balance"))
)
