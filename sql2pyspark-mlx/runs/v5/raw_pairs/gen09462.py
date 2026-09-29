from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("shipped_date").desc_nulls_last(), "id")
w_recent = Window.partitionBy("order_id").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("order_id").orderBy(F.col("discount").desc(), "id")

valid = (
    df_shipments
    .filter(F.col("carrier").isNotNull())
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "cost",
        F.when(F.col("carrier").isin(["Flash", "FedEx"]), "Core").otherwise("Other").alias("carrier_group"),
    )
)

ranked = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

joined = (
    ranked
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id") == F.col("d.id"), "inner")
    .select(
        "a.id",
        "a.order_id",
        "a.shipped_date",
        F.col("d.amount"),
        F.col("d.discount"),
        F.col("d.order_date"),
    )
)

with_windows = (
    joined
    .filter(F.col("discount") >= 0.12)
    .select(
        "*",
        F.sum("discount").over(w_recent).alias("running_sum_discount"),
        F.count("discount").over(w_recent).alias("running_count_discount"),
        F.max("discount").over(w_recent).alias("running_max_discount"),
        F.row_number().over(w_hist).alias("event_seq"),
    )
)

result = (
    with_windows
    .groupBy(
        "order_id",
        F.when(F.col("discount") >= 0.44, "PREMIUM").otherwise("STANDARD").alias("discount_tier"),
    )
    .agg(
        F.max("running_sum_discount").alias("max_running_sum_discount"),
        F.percentile_approx("running_sum_discount", 0.9).alias("running_sum_discount_p90"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("order_id").alias("distinct_order_ids"),
    )
)
