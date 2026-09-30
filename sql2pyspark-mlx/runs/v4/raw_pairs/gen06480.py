from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .filter(F.col("s.carrier").isin(["FedEx", "UPS", "Kerry"]))
    .select(
        F.col("s.order_id"),
        F.col("o.amount"),
        F.col("o.order_date"),
        F.col("o.discount"),
    )
    .orderBy(F.col("order_date").desc())
)
