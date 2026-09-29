from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method", "order_id").orderBy("amount", "id").rowsBetween(-6, Window.currentRow)
w_ordered = Window.partitionBy("status", "method").orderBy("paid_at", "id")
w3 = Window.partitionBy("order_id", "channel").orderBy(F.col("seq_no").desc(), "id")

enriched = (
    df_payments
    .select(
        "*",
        F.max("amount").over(w).alias("last7_max_amount"),
        F.min("amount").over(w).alias("last7_min_amount"),
        F.sum("amount").over(w).alias("last7_sum_amount"),
    )
)

enriched_payments = (
    enriched
    .alias("a")
    .join(df_orders.alias("b"), F.col("a.order_id").eqNullSafe(F.col("b.id")), "left")
    .select("a.*", F.col("b.discount"), F.col("b.order_date"))
)

scored = (
    enriched_payments
    .filter(F.col("order_date") <= "2024-10-01")
    .select(
        "id",
        "order_id",
        "amount",
        "status",
        "last7_max_amount",
        "last7_sum_amount",
        "order_date",
        F.lead("last7_sum_amount").over(w_ordered).alias("next_last7_sum_amount"),
        F.lag("last7_sum_amount").over(w_ordered).alias("prev_last7_sum_amount"),
        F.lead("amount").over(w_ordered).alias("next_amount"),
        F.row_number().over(w_ordered).alias("seq_no"),
    )
)

with_dims = (
    scored
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id") == F.col("b.id"), "left")
    .select(
        "e.id",
        "e.order_id",
        "e.last7_max_amount",
        "e.last7_sum_amount",
        "e.order_date",
        "e.next_last7_sum_amount",
        "e.next_amount",
        "e.seq_no",
        F.col("b.shipping_fee"),
        F.col("b.channel"),
        F.col("b.discount"),
    )
)

ranked = (
    with_dims
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
)

result = (
    ranked
    .groupBy(F.date_trunc("month", "order_date").alias("activity_month"))
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("discount", [0.25, 0.5, 0.75]).alias("discount_quartiles"),
        F.sum(F.when(F.col("channel") == "mobile", F.col("last7_sum_amount")).otherwise(0)).alias("mobile_last7_sum_amount"),
    )
    .orderBy(F.col("activity_month").desc())
)
