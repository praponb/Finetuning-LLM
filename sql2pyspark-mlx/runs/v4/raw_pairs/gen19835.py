from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("amount_tier").orderBy(F.col("amazon_rows").desc())

filtered = (
    df_transactions
    .filter(F.col("txn_time") > F.to_date(F.lit("2023-12-22")))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "merchant",
        F.when(F.col("txn_type") == "CREDIT", 1).otherwise(0).alias("is_credit"),
        F.coalesce("merchant", F.lit("DIRECT")).alias("resolved_merchant"),
    )
)

result = (
    filtered
    .groupBy(
        F.when(F.col("amount") >= 3100, "HIGH").otherwise("LOW").alias("amount_tier"),
        "merchant",
    )
    .agg(
        F.percentile_approx("is_credit", 0.95).alias("is_credit_p95"),
        F.count("*").alias("num_rows"),
        F.count(F.when(F.col("merchant") == "Amazon", 1)).alias("amazon_rows"),
    )
    .withColumn("amazon_rows_rank", F.rank().over(w))
)
