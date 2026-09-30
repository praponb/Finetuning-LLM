from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.order_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.discount").desc(), F.col("c.id").desc())
w_running = Window.partitionBy("city").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running_2 = Window.partitionBy("region").orderBy("opened_date", "id")
w_recent = Window.orderBy(F.col("running_avg_discount").desc_nulls_last())
w6 = Window.partitionBy("city", "region")

with_last = (
    df_stores
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.discount"))
)

with_last_stores = (
    with_last
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.shipping_fee").alias("order_shipping_fee"),
        F.col("c.amount").alias("order_amount"),
    )
)

enriched = (
    with_last_stores
    .filter(F.col("discount") < 0.24)
    .withColumn("running_sum_discount", F.sum("discount").over(w_running))
    .withColumn("running_avg_discount", F.avg("discount").over(w_running))
    .withColumn("running_max_discount", F.max("discount").over(w_running))
    .withColumn("running_count_discount", F.count("discount").over(w_running))
    .withColumn("next_order_amount", F.lead("order_amount", 1).over(w_running_2))
)

kept_keys = (
    enriched
    .select("city", "id")
    .subtract(
        df_stores
        .filter(F.col("opened_date") >= "2024-01-15")
        .select("city", "id")
    )
)

reconciled = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.city") == F.col("k.city")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_windows = (
    reconciled
    .select(
        "id",
        "store_name",
        "region",
        "order_shipping_fee",
        "running_sum_discount",
        F.cume_dist().over(w_recent).alias("running_avg_discount_cume_dist"),
        F.stddev_samp("discount").over(w6).alias("city_stddev_samp_discount"),
    )
)

result = (
    with_windows
    .groupBy("region")
    .agg(
        F.min("running_sum_discount").alias("min_running_sum_discount"),
        F.avg("city_stddev_samp_discount").alias("avg_city_stddev_samp_discount"),
        F.count_if(F.col("region") == "North").alias("north_events"),
    )
)
