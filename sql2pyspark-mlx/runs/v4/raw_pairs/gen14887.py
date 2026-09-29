from pyspark.sql import functions as F

unit_price_by_order_id = df_order_items.groupBy("order_id").agg(F.sum("unit_price").alias("sum_order_item_unit_price"))

remaining_keys = (
    df_payments
    .select("order_id")
    .intersectAll(
        df_payments
        .filter(F.col("amount") >= 1190)
        .select("order_id")
    )
)

reconciled = (
    df_payments
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .join(unit_price_by_order_id, on="order_id", how="left")
    .select("id", "order_id", "method", "status", F.col("sum_order_item_unit_price"))
    .orderBy(F.col("status").desc_nulls_first())
)
