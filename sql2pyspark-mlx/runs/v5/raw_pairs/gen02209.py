from pyspark.sql import functions as F

unit_price_by_order_id = df_order_items.groupBy("order_id").agg(F.sum("unit_price").alias("sum_order_item_unit_price"))

matched_keys = (
    df_payments
    .select("id")
    .intersectAll(
        df_payments
        .filter(F.col("method").isin(["promptpay", "paypal", "cash"]))
        .select("id")
    )
)

reconciled = (
    df_payments
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .join(unit_price_by_order_id, on="order_id", how="left")
    .select("id", "order_id", "amount", "status", F.col("sum_order_item_unit_price"))
)
