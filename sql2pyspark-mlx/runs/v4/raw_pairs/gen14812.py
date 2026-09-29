from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w2 = Window.partitionBy("order_id", "status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("amount", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("order_id", "method").orderBy("amount", "id")
w6 = Window.partitionBy("status").orderBy("paid_at", "id")
w7 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("status").orderBy("amount_bucket", "id")
w_running = Window.orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)

cleaned = (
    df_payments
    .filter(F.col("paid_at") > F.lit("2023-11-30").cast("date"))
    .select("*", F.col("amount").cast("int").alias("amount_int"))
)

valid = (
    cleaned
    .filter(F.col("paid_at") >= F.lit("2024-01-26").cast("date"))
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        "amount_int",
        F.substring("method", 1, 2).alias("method_prefix"),
        F.coalesce("method", F.lit("N/A")).alias("method_clean"),
    )
)

enriched = (
    valid
    .select(
        "*",
        F.sum("amount_int").over(w).alias("amount_int_sum_30d"),
        F.sum("amount_int").over(w2).alias("running_amount_int"),
        F.avg("amount").over(w3).alias("amount_avg_3"),
        F.first("method", ignorenulls=True).over(w4).alias("first_known_method"),
        F.ntile(5).over(w5).alias("amount_bucket"),
    )
)

with_gaps = (
    enriched
    .withColumn("new_group_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w6) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("burst_id", F.sum("new_group_flag").over(w7))

result = (
    sessionized
    .select(
        "*",
        F.row_number().over(w8).alias("seq_no"),
        F.max("amount_bucket").over(w_running).alias("amount_bucket_max_90d"),
    )
)
