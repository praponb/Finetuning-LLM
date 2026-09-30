from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.paid_at").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("store_city").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("payment_status").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_orders
    .filter(F.col("customer_id").isNotNull() & (F.col("order_date") >= F.lit("2023-08-04").cast("date")))
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        F.initcap(F.trim("channel")).alias("channel_title"),
        (F.col("discount") - F.col("shipping_fee")).alias("discount_minus_shipping_fee"),
        F.coalesce("channel", F.lit("N/A")).alias("channel_filled"),
    )
)

with_latest = (
    valid
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.paid_at") <= F.col("s.order_date")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.store_id",
        "s.channel_filled",
        F.col("c.status").alias("payment_status"),
        F.col("c.method").alias("payment_method"),
    )
)

enriched = (
    with_latest
    .alias("e")
    .join(df_stores.alias("p"), F.col("e.store_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.sqft"), F.col("p.city"))
)

enriched_orders = (
    enriched
    .alias("s")
    .join(df_stores.alias("p2"), F.col("s.store_id") == F.col("p2.id"), "left")
    .select(
        "s.id",
        "s.store_id",
        "s.channel_filled",
        "s.payment_status",
        "s.city",
        F.col("p2.opened_date"),
        F.col("p2.city").alias("store_city"),
    )
)

with_windows = (
    enriched_orders
    .select(
        "id",
        "store_id",
        "channel_filled",
        "city",
        F.first("store_city", ignorenulls=True).over(w2).alias("first_known_store_city"),
        F.last("payment_status", ignorenulls=True).over(w3).alias("last_known_payment_status"),
        F.first("city", ignorenulls=True).over(w4).alias("first_known_city"),
    )
)

base = (
    with_windows
    .filter(F.col("store_id").isin([25, 34]))
    .withColumn("city_lower", F.lower("city"))
)

with_dims = (
    base
    .alias("a")
    .join(df_stores.alias("b"), F.col("a.store_id") == F.col("b.id"), "left")
    .select(
        "a.id",
        "a.store_id",
        "a.first_known_store_city",
        "a.city_lower",
        F.col("b.city").alias("store_city"),
        F.col("b.region"),
    )
)

result = (
    with_dims
    .groupingSets(
        [["store_city", "region"], ["store_city"], []],
        "store_city",
        "region",
    )
    .agg(
        F.size(F.collect_set("store_city")).alias("num_store_cities"),
        F.count_if(F.col("region") == "Central").alias("central_events"),
        F.sort_array(F.collect_set("region")).alias("region_list"),
        F.size(F.collect_set("store_city")).alias("num_store_cities_2"),
        F.count("*").alias("row_count"),
    )
)
