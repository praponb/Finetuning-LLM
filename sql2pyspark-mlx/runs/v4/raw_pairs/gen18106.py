from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("amount", "id").rowsBetween(-6, -1)
w2 = Window.partitionBy("status").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("amount_whole", "id").rowsBetween(-6, -1)
w4 = Window.orderBy("amount", "id")
w5 = Window.partitionBy("order_id", "status").orderBy("amount_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("order_id").orderBy("paid_at", "id")
w7 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("amount")
w9 = Window.partitionBy("order_id").orderBy("amount_avg_6_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("status").orderBy("amount", "id")
w11 = Window.partitionBy("method")
w12 = Window.orderBy("row_seq", "id")
w13 = Window.partitionBy("status").orderBy("amount_bucket", "id").rowsBetween(-3, -1)
w14 = Window.partitionBy("order_id").orderBy("prev_burst_id", "id")
w15 = Window.partitionBy("method", "order_id")
cost_by_order_id = df_shipments.groupBy("order_id").agg(F.max("cost").alias("max_shipment_cost"))

filtered = (
    df_payments
    .filter(F.col("status") == "REFUNDED")
    .withColumn("amount_whole", F.floor("amount"))
)

enriched = (
    filtered
    .select(
        "*",
        F.avg("amount").over(w).alias("amount_avg_6_prior"),
        F.avg("amount").over(w2).alias("amount_avg_7d"),
        F.avg("amount_whole").over(w3).alias("amount_whole_avg_6_prior"),
        F.row_number().over(w4).alias("row_seq"),
        F.sum("amount_whole").over(w5).alias("running_amount_whole"),
    )
)

gaps = (
    enriched
    .withColumn("is_new_burst", F.when(F.col("paid_at") - F.lag("paid_at").over(w6) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

sessions_payments = gaps.withColumn("burst_id", F.sum("is_new_burst").over(w7))

metrics = (
    sessions_payments
    .withColumn("prev_burst_id", F.lag("burst_id", 1, 0).over(w4))
    .withColumn("amount_rank", F.rank().over(w8))
    .withColumn("last_known_is_new_burst", F.last("is_new_burst", ignorenulls=True).over(w9))
    .withColumn("amount_bucket", F.ntile(5).over(w10))
)

scored = (
    metrics
    .select(
        "*",
        F.stddev_samp("amount_avg_6_prior").over(w11).alias("method_stddev_samp_amount_avg_6_prior"),
        F.ntile(3).over(w12).alias("row_seq_bucket"),
        F.avg("running_amount_whole").over(w13).alias("running_amount_whole_avg_3_prior"),
        F.ntile(3).over(w14).alias("prev_burst_id_bucket"),
        F.min("amount_avg_6_prior").over(w15).alias("method_min_amount_avg_6_prior"),
    )
)

result = (
    scored
    .join(cost_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "method",
        "status",
        "amount_whole",
        "amount_avg_6_prior",
        "amount_avg_7d",
        "amount_whole_avg_6_prior",
        "row_seq",
        F.col("max_shipment_cost"),
    )
    .orderBy(F.col("amount_avg_6_prior").desc())
)
