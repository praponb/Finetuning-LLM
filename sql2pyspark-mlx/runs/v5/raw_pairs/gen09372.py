from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("store_id").orderBy("shipping_fee", "id")
w2 = Window.partitionBy("store_id").orderBy("amount", "id")
w3 = Window.partitionBy("status").orderBy("discount", "id")
w4 = Window.partitionBy("status").orderBy("prev_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("customer_id").orderBy("prev_amount", "id")

metrics = (
    df_orders
    .withColumn("prev_shipping_fee", F.lag("shipping_fee").over(w_recent))
    .withColumn("next_amount", F.lead("amount").over(w_recent))
    .withColumn("prev_amount", F.lag("amount").over(w_recent))
    .withColumn("seq_no", F.row_number().over(w_recent))
)

not_flagged = (
    metrics
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.quantity") < 5), "left_anti")
)

deduped = (
    not_flagged
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "customer_id",
        "status",
        "amount",
        "discount",
        "next_amount",
        "prev_amount",
    )
)

base = (
    deduped
    .filter(F.col("amount") > 530)
    .select(
        "id",
        "customer_id",
        "status",
        "discount",
        "next_amount",
        "prev_amount",
        F.round((F.col("discount") / F.col("next_amount")), 2).alias("discount_per_next_amount"),
    )
)

enriched = (
    base
    .filter(F.col("prev_amount") <= 530)
    .select(
        "id",
        "customer_id",
        "discount",
        "discount_per_next_amount",
        F.ntile(5).over(w3).alias("discount_bucket"),
        F.sum("discount_per_next_amount").over(w4).alias("cumulative_discount_per_next_amount"),
        F.lead("discount_per_next_amount", 1).over(w_running).alias("next_discount_per_next_amount"),
    )
)

result = (
    enriched
    .groupBy("customer_id")
    .agg(
        F.percentile_approx("discount_per_next_amount", 0.5).alias("discount_per_next_amount_p50"),
        F.percentile_approx("discount_bucket", [0.25, 0.5, 0.75]).alias("discount_bucket_quartiles"),
        F.count("*").alias("cnt"),
        F.round(F.avg("discount"), 2).alias("avg_discount"),
    )
    .orderBy("discount_per_next_amount_p50")
)
