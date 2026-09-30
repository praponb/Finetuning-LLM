from pyspark.sql import functions as F

cost_by_order_id = df_shipments.groupBy("order_id").agg(F.max("cost").alias("max_shipment_cost"))

matched_keys = (
    df_order_items
    .select("id", "order_id")
    .subtract(
        df_order_items
        .filter(F.col("quantity") == 6)
        .select("id", "order_id")
    )
)

verified = (
    df_order_items
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .join(cost_by_order_id, on="order_id", how="left")
    .select("order_id", "product_id", "unit_price", F.col("max_shipment_cost"))
)
