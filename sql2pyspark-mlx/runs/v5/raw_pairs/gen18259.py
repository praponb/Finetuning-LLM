from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("discount", "id")
w_grp = Window.orderBy(F.col("discount").desc())
w_grp_2 = Window.partitionBy("status", "customer_id").orderBy("discount", "id")
w4 = Window.partitionBy("customer_id", "status").orderBy("discount_rank", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.partitionBy("channel").orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("status", "channel").orderBy("last3_sum_discount_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("channel").orderBy(F.col("shipping_fee").desc())

valid = (
    df_orders
    .filter((F.col("order_date") > F.lit("2023-06-12").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.coalesce("channel", F.lit("N/A")).alias("channel_filled"),
        F.coalesce("channel", F.lit("NONE")).alias("channel_filled_2"),
    )
)

deduped = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

metrics = (
    deduped
    .select(
        "*",
        F.rank().over(w_grp).alias("discount_rank"),
        F.lag("shipping_fee", 1).over(w_grp_2).alias("prev_shipping_fee"),
    )
)

enriched = (
    metrics
    .filter(F.col("discount_rank") != 4)
    .select(
        "id",
        "order_date",
        "status",
        "channel",
        "amount",
        "shipping_fee",
        F.avg("discount_rank").over(w4).alias("last3_avg_discount_rank"),
        F.max("discount_rank").over(w4).alias("last3_max_discount_rank"),
        F.sum("discount_rank").over(w4).alias("last3_sum_discount_rank"),
        F.sum("prev_shipping_fee").over(w5).alias("cumulative_prev_shipping_fee"),
    )
)

base = (
    enriched
    .filter((F.col("order_date") > F.lit("2023-02-20").cast("date")) & (F.col("cumulative_prev_shipping_fee") == 158))
    .select(
        "id",
        "status",
        "channel",
        "amount",
        "shipping_fee",
        "last3_avg_discount_rank",
        "last3_max_discount_rank",
        "last3_sum_discount_rank",
        "cumulative_prev_shipping_fee",
        F.col("amount").cast("int").alias("amount_int"),
        F.round((F.col("cumulative_prev_shipping_fee") / F.col("last3_avg_discount_rank")), 2).alias("cumulative_prev_shipping_fee_per_last3_avg_discount_rank"),
        F.coalesce("channel", F.lit("DIRECT")).alias("channel_clean"),
    )
)

metrics_orders = (
    base
    .select(
        "id",
        "channel",
        "amount",
        "shipping_fee",
        "last3_max_discount_rank",
        "cumulative_prev_shipping_fee",
        "cumulative_prev_shipping_fee_per_last3_avg_discount_rank",
        "channel_clean",
        F.last("amount", ignorenulls=True).over(w_ordered).alias("last_known_amount"),
        F.dense_rank().over(w7).alias("shipping_fee_dense_rank"),
    )
)

result = (
    metrics_orders
    .select(
        "id",
        "channel",
        "amount",
        "last3_max_discount_rank",
        "channel_clean",
        "last_known_amount",
        "shipping_fee_dense_rank",
        F.when(F.col("channel").rlike("^(web|partner)$"), "CORE").when(F.col("channel").rlike("^(mobile|store)$"), "GROUP_A").otherwise("EXTENDED").alias("channel_zone"),
    )
)
