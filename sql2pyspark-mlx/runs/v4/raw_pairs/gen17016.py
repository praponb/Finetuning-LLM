from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .filter((F.col("s.carrier") == "DHL") & F.col("o.status").isin(["RETURNED", "PENDING"]))
    .select(
        F.col("s.carrier"),
        F.col("s.shipped_date"),
        F.col("o.store_id"),
        F.date_add(F.col("s.delivered_date"), 78).alias("delivered_date_plus_78d"),
    )
)
