from pyspark.sql import functions as F

cost_by_order_id = df_shipments.groupBy("order_id").agg(F.max("cost").alias("max_shipment_cost"))

remaining_keys = (
    df_shipments
    .select("status", "id")
    .exceptAll(
        df_shipments
        .filter(F.month("delivered_date").isin([4, 5, 10]))
        .select("status", "id")
    )
)

kept_rows = (
    df_shipments
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .join(cost_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "carrier",
        "delivered_date",
        "cost",
        "status",
        F.when(F.col("status").rlike("^(DELIVERED|IN_TRANSIT)$"), "EXTENDED").when(F.col("status").rlike("^(LOST|RETURNED)$"), "CORE").otherwise("OTHER_ZONE").alias("status_zone"),
        F.col("max_shipment_cost"),
    )
)
