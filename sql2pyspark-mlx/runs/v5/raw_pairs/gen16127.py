from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.orderBy("paid_at", "id").rowsBetween(-6, -1)
w_part = Window.partitionBy("method")
w4 = Window.partitionBy("status").orderBy("amount_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part_2 = Window.partitionBy("order_id", "method").orderBy("amount_filled", "id")
w6 = Window.partitionBy("method").orderBy("running_amount", "id")
w7 = Window.partitionBy("order_id")
w8 = Window.partitionBy("status", "order_id").orderBy(F.col("amount_filled_bucket").desc(), "id")
w9 = Window.partitionBy("status").orderBy("cumulative_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("order_id").orderBy("cumulative_amount", "id")

valid = (
    df_payments
    .filter((F.col("paid_at") > F.lit("2023-06-10").cast("date")) & F.col("method").isNotNull())
    .select("*", F.coalesce("amount", F.lit(0)).alias("amount_filled"))
)

metrics = (
    valid
    .filter(F.col("order_id").isNotNull())
    .select(
        "*",
        F.sum("amount").over(w).alias("running_amount"),
        F.avg("amount_filled").over(w_running).alias("amount_filled_avg_6_prior"),
        F.max("amount_filled").over(w_part).alias("method_max_amount_filled"),
        F.sum("amount").over(w4).alias("cumulative_amount"),
        F.ntile(5).over(w_part_2).alias("amount_filled_bucket"),
    )
)

enriched = (
    metrics
    .withColumn("running_amount_bucket", F.ntile(10).over(w6))
    .withColumn("order_id_avg_amount_filled_avg_6_prior", F.avg("amount_filled_avg_6_prior").over(w7))
)

ranked = (
    enriched
    .withColumn("latest_rank", F.row_number().over(w8))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "order_id",
        "status",
        "running_amount",
        "amount_filled_avg_6_prior",
        "cumulative_amount",
        "amount_filled_bucket",
        "order_id_avg_amount_filled_avg_6_prior",
    )
)

result = (
    ranked
    .select(
        "id",
        "status",
        "running_amount",
        "amount_filled_avg_6_prior",
        "cumulative_amount",
        F.last("amount_filled_avg_6_prior", ignorenulls=True).over(w9).alias("last_known_amount_filled_avg_6_prior"),
        F.lag("amount_filled_bucket", 2, 0).over(w10).alias("prev_amount_filled_bucket"),
    )
)
