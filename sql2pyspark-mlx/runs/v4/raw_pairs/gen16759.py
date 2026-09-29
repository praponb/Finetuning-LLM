from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("status").orderBy("discount", "id")
w2 = Window.partitionBy("order_status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("order_id").orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("order_status").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w_hist = Window.partitionBy("status").orderBy("running_discount", "id")
w6 = Window.partitionBy("method").orderBy("prev_discount", "id")
w7 = Window.orderBy(F.col("amount").desc())
w8 = Window.partitionBy("order_status").orderBy("discount", "id").rowsBetween(-6, -1)

with_dims = (
    df_payments
    .alias("a")
    .join(df_orders.alias("p"), F.col("a.order_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.discount"), F.col("p.status").alias("order_status"))
)

scored = (
    with_dims
    .withColumn("discount_bucket", F.ntile(5).over(w_recent))
    .withColumn("running_discount", F.sum("discount").over(w2))
)

with_windows = (
    scored
    .select(
        "*",
        F.sum("running_discount").over(w3).alias("running_running_discount"),
        F.avg("discount").over(w_grp).alias("discount_avg_30d"),
        F.lag("discount", 1, 0).over(w_hist).alias("prev_discount"),
    )
)

enriched = (
    with_windows
    .select(
        "*",
        F.ntile(5).over(w6).alias("prev_discount_bucket"),
        F.dense_rank().over(w7).alias("amount_dense_rank"),
        F.avg("discount_avg_30d").over(w8).alias("discount_avg_30d_avg_6_prior"),
    )
)

result = (
    enriched
    .withColumn("week_start", F.date_trunc("week", "paid_at"))
    .withColumn("prev_discount_bucket_tier", F.when(F.col("prev_discount_bucket") >= 8, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["week_start", "prev_discount_bucket_tier"], ["week_start"], []],
        "week_start",
        "prev_discount_bucket_tier",
    )
    .agg(
        F.round(F.avg("amount_dense_rank"), 2).alias("avg_amount_dense_rank"),
        F.count("*").alias("num_rows"),
        F.avg("discount_bucket").alias("avg_discount_bucket"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.size(F.collect_set("status")).alias("num_status_2"),
        F.sum(F.when(F.col("status") == "SUCCESS", F.col("amount_dense_rank")).otherwise(0)).alias("success_amount_dense_rank"),
    )
)
