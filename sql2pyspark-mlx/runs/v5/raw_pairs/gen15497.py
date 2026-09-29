from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("method", "status").orderBy("amount", "id").rowsBetween(-3, Window.currentRow)
w_hist = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("method").orderBy("last4_max_amount", "id").rowsBetween(-6, -1)
w4 = Window.partitionBy("status").orderBy(F.col("paid_at").desc(), "id")
w5 = Window.partitionBy("order_id", "method").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w6 = Window.partitionBy("status", "order_id").orderBy("last_known_amount", "id")
w7 = Window.partitionBy("status").orderBy("last_known_amount", "id")

base = (
    df_payments
    .filter(F.col("paid_at") > F.lit("2023-06-10").cast("date"))
    .withColumn("method_lower", F.lower("method"))
)

base_payments = (
    base
    .filter(~F.col("method").isin(["cash", "paypal", "bank_transfer"]) & (F.col("paid_at") >= F.lit("2023-05-04 14:00:00").cast("timestamp")))
    .withColumn("status_prefix", F.substring("status", 1, 3))
)

with_windows = (
    base_payments
    .withColumn("last4_max_amount", F.max("amount").over(w_running))
    .withColumn("last4_avg_amount", F.avg("amount").over(w_running))
    .withColumn("last4_sum_amount", F.sum("amount").over(w_running))
    .withColumn("last_known_amount", F.last("amount", ignorenulls=True).over(w_hist))
)

enriched = (
    with_windows
    .filter(F.col("amount") > 800)
    .select(
        "*",
        F.avg("last4_sum_amount").over(w3).alias("last4_sum_amount_avg_6_prior"),
        F.row_number().over(w4).alias("seq_no"),
    )
)

result = (
    enriched
    .select(
        "id",
        "order_id",
        "status",
        "method_lower",
        "status_prefix",
        "last4_avg_amount",
        "last4_sum_amount",
        "last_known_amount",
        "seq_no",
        F.count("seq_no").over(w5).alias("seq_no_count_90d"),
        F.row_number().over(w6).alias("seq_no_2"),
        F.ntile(3).over(w7).alias("last_known_amount_bucket"),
    )
)
