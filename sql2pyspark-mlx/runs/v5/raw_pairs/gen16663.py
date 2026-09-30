from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .filter((F.col("o.channel") != "web") & (F.year(F.col("o.order_date")) == 2023))
    .select(
        F.col("s.shipped_date"),
        F.col("s.order_id"),
        F.col("s.carrier"),
        F.col("o.channel"),
    )
    .orderBy("shipped_date")
)
