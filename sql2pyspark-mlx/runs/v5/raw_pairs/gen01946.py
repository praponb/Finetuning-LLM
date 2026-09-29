from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "status").orderBy("delivered_date", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy(F.col("shipping_fee").desc(), "id")
unit_price_by_order_id = df_order_items.groupBy("order_id").agg(F.min("unit_price").alias("min_order_item_unit_price"))

valid = (
    df_shipments
    .filter(F.col("order_id").isNotNull() & F.month("shipped_date").isin([7, 8, 9]))
    .select(
        "*",
        F.when(F.col("status").isin(["DELIVERED", "IN_TRANSIT"]), "Core").otherwise("Other").alias("status_group"),
        F.coalesce("carrier", F.lit("DIRECT")).alias("carrier_filled"),
    )
)

enriched = (
    valid
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id") == F.col("p.id"), "left")
    .filter(F.col("p.store_id").isNotNull())
    .select(
        "src.id",
        "src.order_id",
        "src.delivered_date",
        "src.cost",
        "src.status",
        "src.carrier_filled",
        F.col("p.shipping_fee"),
        F.col("p.discount"),
    )
)

enriched_shipments = (
    enriched
    .select(
        "*",
        F.sum("cost").over(w).alias("last7_sum_cost"),
        F.avg("cost").over(w).alias("last7_avg_cost"),
        F.max("cost").over(w).alias("last7_max_cost"),
        F.min("cost").over(w).alias("last7_min_cost"),
        F.ntile(10).over(w2).alias("shipping_fee_bucket"),
    )
)

joined = (
    enriched_shipments
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id").eqNullSafe(F.col("b.id")), "inner")
    .select("e.*", F.col("b.order_date"))
)

enriched_shipments_2 = (
    joined
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id") == F.col("b.id"), "left")
    .select(
        "e.*",
        F.col("b.shipping_fee").alias("order_shipping_fee"),
        F.col("b.channel"),
    )
)

result = (
    enriched_shipments_2
    .join(unit_price_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "delivered_date",
        "status",
        "shipping_fee",
        "discount",
        "last7_sum_cost",
        "last7_avg_cost",
        "last7_max_cost",
        F.when(F.col("status").rlike("^(LOST|DELIVERED)$"), "GROUP_A").when(F.col("status").rlike("^(IN_TRANSIT|RETURNED)$"), "EXTENDED").otherwise("PRIORITY").alias("status_zone"),
        F.col("min_order_item_unit_price"),
    )
    .orderBy(F.col("discount").desc())
)
