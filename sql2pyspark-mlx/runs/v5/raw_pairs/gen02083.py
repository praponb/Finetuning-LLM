from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("order_order_date").desc_nulls_first())

base = (
    df_shipments
    .filter(F.col("shipped_date") > F.to_date(F.lit("2023-07-08")))
    .select(
        "id",
        "order_id",
        "carrier",
        "delivered_date",
        "cost",
        F.lower("carrier").alias("carrier_lower"),
        F.length("status").alias("status_length"),
        F.coalesce("carrier", F.lit("NONE")).alias("carrier_clean"),
    )
)

joined = (
    base
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id").eqNullSafe(F.col("o.id")), "left")
    .select(
        "src.id",
        "src.order_id",
        "src.delivered_date",
        "src.carrier_lower",
        "src.carrier_clean",
        F.col("o.order_date"),
        F.col("o.discount"),
        F.col("o.amount"),
    )
)

with_dims = (
    joined
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id") == F.col("b.id"), "inner")
    .filter(F.col("b.status") == "CANCELLED")
    .select(
        "src.id",
        "src.order_id",
        "src.carrier_clean",
        "src.discount",
        F.col("b.shipping_fee"),
        F.col("b.order_date").alias("order_order_date"),
    )
)

latest = (
    with_dims
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

result = (
    latest
    .select(
        "id",
        "order_id",
        "carrier_clean",
        "discount",
        "shipping_fee",
        "order_order_date",
    )
    .orderBy(F.col("order_id").desc())
)
