from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method", "order_id").orderBy("amount", "id")
w2 = Window.partitionBy("order_id").orderBy("dedup_rank", "id")
w3 = Window.partitionBy("order_id").orderBy(F.col("amount").desc(), "id")

valid = (
    df_payments
    .filter((F.col("paid_at") >= F.lit("2023-10-26 12:00:00").cast("timestamp")) & F.col("method").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "status",
        F.coalesce("amount", F.lit(0)).alias("amount_filled"),
    )
)

base = (
    valid
    .filter(F.col("order_id").isNotNull())
    .withColumn("status_lower", F.lower("status"))
    .withColumn("status_prefix", F.substring("status", 1, 3))
)

latest = (
    base
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

ranked = (
    latest
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
)

latest_payments = (
    ranked
    .withColumn("row_num_2", F.row_number().over(w3))
    .filter(F.col("row_num_2") == 1)
)

result = (
    latest_payments
    .groupBy("method")
    .pivot("status", ["REFUNDED", "SUCCESS"])
    .agg(F.max("amount").alias("value_sum"), F.count(F.lit(1)).alias("cnt"))
    .select(
        "method",
        F.col("REFUNDED_value_sum").alias("refunded_value_sum"),
        F.col("REFUNDED_cnt").alias("refunded_cnt"),
        F.col("SUCCESS_value_sum").alias("success_value_sum"),
        F.col("SUCCESS_cnt").alias("success_cnt"),
    )
)
