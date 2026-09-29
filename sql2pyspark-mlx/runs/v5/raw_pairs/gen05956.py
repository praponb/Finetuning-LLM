from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "inner")
    .groupBy(F.col("o.channel"), F.year(F.col("o.order_date")).alias("order_year"))
    .agg(F.countDistinct(F.col("s.cost")).alias("unique_carriers"))
    .filter(F.col("unique_carriers") >= 4)
)
