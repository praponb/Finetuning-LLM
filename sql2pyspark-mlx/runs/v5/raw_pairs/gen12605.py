from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id"), F.col("s.promo_id")).orderBy(F.col("c.shipped_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("status").orderBy("discount", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("promo_code").orderBy("promo_id", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status")
w5 = Window.orderBy("discount", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("customer_id")
w7 = Window.partitionBy("channel", "promo_code").orderBy("discount", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy(F.col("discount_plus_amount").desc_nulls_first())
w9 = Window.partitionBy("customer_id").orderBy("running_avg_first_known_shipping_fee", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("promo_code").orderBy(F.col("discount_times_shipping_fee").desc(), "id", "promo_id")
w_part = Window.partitionBy("channel", "promo_code").orderBy("order_date", "id", "promo_id")
w12 = Window.partitionBy("customer_id").orderBy("running_min_first_known_shipping_fee", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_orders
    .filter(F.col("order_date") >= F.to_date(F.lit("2023-02-24")))
    .select(
        "id",
        "customer_id",
        "order_date",
        "status",
        "channel",
        "discount",
        "shipping_fee",
        (F.col("discount") + F.col("amount")).alias("discount_plus_amount"),
        (F.col("discount") * F.col("shipping_fee")).alias("discount_times_shipping_fee"),
        F.coalesce("channel", F.lit("OTHER")).alias("resolved_channel"),
    )
)

with_window = (
    valid
    .alias("src")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("src.channel"), F.lit("store")) == F.col("pr.channel")) & (F.col("src.order_date") >= F.col("pr.starts_on")) & (F.col("src.order_date") <= F.date_add(F.col("pr.ends_on"), 10)), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

latest_match = (
    with_window
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.shipped_date") <= F.col("s.order_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.shipped_date"), F.col("c.status").alias("shipment_status"))
)

enriched = (
    latest_match
    .withColumn("cumulative_discount", F.sum("discount").over(w2))
    .withColumn("first_known_shipping_fee", F.first("shipping_fee", ignorenulls=True).over(w3))
    .withColumn("status_sum_shipping_fee", F.sum("shipping_fee").over(w4))
    .withColumn("running_discount", F.sum("discount").over(w5))
    .withColumn("customer_id_max_discount_plus_amount", F.max("discount_plus_amount").over(w6))
)

with_windows = (
    enriched
    .select(
        "id",
        "customer_id",
        "order_date",
        "channel",
        "discount",
        "shipping_fee",
        "discount_plus_amount",
        "discount_times_shipping_fee",
        "resolved_channel",
        "promo_id",
        "promo_code",
        "discount_pct",
        "shipped_date",
        F.sum("first_known_shipping_fee").over(w7).alias("running_sum_first_known_shipping_fee"),
        F.avg("first_known_shipping_fee").over(w7).alias("running_avg_first_known_shipping_fee"),
        F.min("first_known_shipping_fee").over(w7).alias("running_min_first_known_shipping_fee"),
        F.dense_rank().over(w8).alias("discount_plus_amount_dense_rank"),
    )
)

with_windows_orders = (
    with_windows
    .select(
        "*",
        F.avg("running_avg_first_known_shipping_fee").over(w9).alias("running_avg_running_avg_first_known_shipping_fee"),
        F.min("running_avg_first_known_shipping_fee").over(w9).alias("running_min_running_avg_first_known_shipping_fee"),
        F.count("running_avg_first_known_shipping_fee").over(w9).alias("running_count_running_avg_first_known_shipping_fee"),
        F.row_number().over(w10).alias("seq_no"),
    )
)

scored = (
    with_windows_orders
    .filter(F.col("running_count_running_avg_first_known_shipping_fee").between(12, 14))
    .select(
        "id",
        "customer_id",
        "order_date",
        "discount_plus_amount",
        "resolved_channel",
        "promo_id",
        "running_min_first_known_shipping_fee",
        "discount_plus_amount_dense_rank",
        "running_min_running_avg_first_known_shipping_fee",
        "running_count_running_avg_first_known_shipping_fee",
        "seq_no",
        F.row_number().over(w_part).alias("row_seq"),
        F.first("running_sum_first_known_shipping_fee", ignorenulls=True).over(w12).alias("first_known_running_sum_first_known_shipping_fee"),
    )
)

result = (
    scored
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "inner")
    .cube(F.col("p.city").alias("city"), F.col("p.segment").alias("segment"))
    .agg(
        F.count("*").alias("rows_total"),
        F.sum(F.col("src.promo_id")).alias("total_promo_id"),
        F.min(F.col("src.discount_plus_amount")).alias("min_discount_plus_amount"),
        F.grouping_id().alias("level_id"),
    )
    .orderBy(F.col("city").desc_nulls_last())
)
