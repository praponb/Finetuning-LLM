from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_transactions
    .filter((F.col("txn_time") < "2024-08-30") & (F.col("txn_time") > F.to_date(F.lit("2023-10-06"))))
    .select(
        "*",
        F.initcap(F.trim("merchant")).alias("merchant_title"),
        F.when(F.col("merchant") == "Shell", 1).otherwise(0).alias("is_shell"),
        F.coalesce("merchant", F.lit("DIRECT")).alias("resolved_merchant"),
    )
)

with_gaps = (
    cleaned
    .withColumn("is_new_session", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessionized = (
    with_gaps
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "resolved_merchant",
        F.sum("is_new_session").over(w2).alias("burst_id"),
    )
)

result = (
    sessionized
    .groupBy(
        F.when(F.col("amount") >= 4500, "HIGH").otherwise("LOW").alias("amount_tier"),
        F.when(F.col("burst_id") >= 6, "PREMIUM").otherwise("STANDARD").alias("burst_id_tier"),
    )
    .agg(
        F.sum("burst_id").alias("total_burst_id"),
        F.count("*").alias("num_rows"),
        F.countDistinct("txn_type").alias("distinct_txn_types"),
    )
)
