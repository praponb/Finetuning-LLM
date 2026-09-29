from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("status", "store_id")
w2 = Window.partitionBy("store_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_orders
    .filter(F.col("channel").isNotNull())
    .select(
        "id",
        "store_id",
        "status",
        "amount",
        "discount",
        "shipping_fee",
        F.date_add("order_date", 70).alias("order_date_plus_70d"),
    )
)

scored = (
    valid
    .select(
        "*",
        F.stddev_samp("shipping_fee").over(w_part).alias("status_stddev_samp_shipping_fee"),
        F.last("shipping_fee", ignorenulls=True).over(w2).alias("last_known_shipping_fee"),
    )
)

with_dims = (
    scored
    .alias("a")
    .join(df_stores.alias("p"), F.col("a.store_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.store_name"), F.col("p.city"), F.col("p.region"))
)

enriched = (
    with_dims
    .alias("e")
    .join(df_stores.alias("b"), F.col("e.store_id") == F.col("b.id"), "left")
    .select(
        "e.*",
        F.col("b.store_name").alias("store_store_name"),
        F.col("b.city").alias("store_city"),
        F.col("b.opened_date"),
    )
)

base = (
    enriched
    .filter(F.col("amount") >= 1260)
    .select(
        "*",
        F.concat_ws(",", "city", "region").alias("city_region_key"),
        F.last_day("opened_date").alias("opened_date_month_end"),
    )
)

with_dims_orders = (
    base
    .alias("a")
    .join(df_stores.alias("d"), F.col("a.store_id") == F.col("d.id"), "inner")
    .select(
        "a.*",
        F.col("d.opened_date").alias("store_opened_date"),
        F.col("d.store_name").alias("store_store_name_2"),
        F.col("d.city").alias("store_city_2"),
    )
)

with_dims_orders_2 = (
    with_dims_orders
    .alias("e")
    .join(df_stores.alias("b"), F.col("e.store_id") == F.col("b.id"), "left")
    .select(
        "e.*",
        F.col("b.city").alias("store_city_3"),
        F.col("b.sqft"),
        F.col("b.region").alias("store_region"),
    )
)

grouped = (
    with_dims_orders_2
    .groupBy(F.date_trunc("QUARTER", "opened_date_month_end").alias("quarter_start"))
    .agg(
        F.percentile_approx("shipping_fee", [0.25, 0.5, 0.75]).alias("shipping_fee_quartiles"),
        F.corr("discount", "shipping_fee").alias("corr_discount_shipping_fee"),
        F.sum(F.when(F.col("store_city_3") == "Tokyo", F.col("amount")).otherwise(0)).alias("tokyo_amount"),
    )
)

result = (
    grouped
    .select(
        "quarter_start",
        "shipping_fee_quartiles",
        "corr_discount_shipping_fee",
        "tokyo_amount",
    )
    .orderBy(F.col("tokyo_amount").asc_nulls_last())
)
