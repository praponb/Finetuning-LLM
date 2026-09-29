from pyspark.sql import functions as F

cost_by_order_id = df_shipments.groupBy("order_id").agg(F.sum("cost").alias("sum_shipment_cost"))

remaining_keys = (
    df_shipments
    .select("status", "order_id")
    .exceptAll(
        df_shipments
        .filter(F.col("order_id").isNull())
        .select("status", "order_id")
    )
)

reconciled = (
    df_shipments
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .join(cost_by_order_id, on="order_id", how="left")
    .select("order_id", "shipped_date", "delivered_date", F.col("sum_shipment_cost"))
    .orderBy(F.col("shipped_date").asc_nulls_first(), "delivered_date")
)
