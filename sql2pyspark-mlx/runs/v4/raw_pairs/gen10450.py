from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("paid_at").desc_nulls_first(), "id")
unit_price_by_order_id = df_order_items.groupBy("order_id").agg(F.max("unit_price").alias("max_order_item_unit_price"))

filtered = (
    df_payments
    .filter((F.col("paid_at") >= F.lit("2023-10-06 11:00:00").cast("timestamp")) & (F.col("method") == "card"))
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        F.concat("method", F.lit(", "), "status").alias("method_status"),
    )
)

ranked = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

enriched = (
    ranked
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.order_date"), F.col("d.discount"))
)

daily = (
    enriched
    .groupBy("order_id")
    .agg(
        F.avg("discount").alias("avg_discount"),
        F.max("discount").alias("max_discount"),
        F.max("paid_at").alias("last_paid_at"),
    )
)

result = (
    daily
    .join(unit_price_by_order_id, on="order_id", how="left")
    .select(
        "order_id",
        "max_discount",
        "last_paid_at",
        F.col("max_order_item_unit_price"),
    )
)
