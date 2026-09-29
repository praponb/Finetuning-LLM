from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("delivered_date", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("status", "channel").orderBy(F.col("cost").desc(), "id")
unit_price_by_order_id = df_order_items.groupBy("order_id").agg(F.avg("unit_price").alias("avg_order_item_unit_price"))

base = (
    df_shipments
    .filter(F.col("order_id").isNotNull() & (F.col("status") == "LOST"))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        F.round("cost", 0).alias("cost_rounded"),
        F.coalesce("carrier", F.lit("UNKNOWN")).alias("carrier_filled"),
    )
)

joined = (
    base
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id").eqNullSafe(F.col("d.id")), "inner")
    .select("a.*", F.col("d.status"), F.col("d.shipping_fee"), F.col("d.channel"))
)

base_shipments = (
    joined
    .filter((F.col("delivered_date") > F.lit("2023-04-26").cast("date")) & F.month("delivered_date").isin([4, 9, 12]))
    .select(
        "*",
        F.when(F.col("channel") == "store", 1).otherwise(0).alias("is_store"),
        F.length("status").alias("status_length"),
    )
)

metrics = (
    base_shipments
    .select(
        "*",
        F.avg("is_store").over(w).alias("last7_avg_is_store"),
        F.count("is_store").over(w).alias("last7_count_is_store"),
        F.min("is_store").over(w).alias("last7_min_is_store"),
        F.row_number().over(w2).alias("row_seq"),
    )
)

cleaned = (
    metrics
    .filter((F.col("delivered_date") > F.lit("2023-05-25").cast("date")) & F.col("carrier").isNotNull())
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "cost_rounded",
        "carrier_filled",
        "status",
        "channel",
        "is_store",
        "status_length",
        "last7_avg_is_store",
        "last7_count_is_store",
        "row_seq",
        F.ceil("last7_count_is_store").alias("last7_count_is_store_whole"),
    )
)

result = (
    cleaned
    .join(unit_price_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "carrier",
        "delivered_date",
        "cost_rounded",
        "status",
        "channel",
        "is_store",
        "status_length",
        F.col("avg_order_item_unit_price"),
    )
)
