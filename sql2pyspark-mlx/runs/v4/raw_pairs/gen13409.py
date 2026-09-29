from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .groupBy(F.year(F.col("o.order_date")).alias("order_year"))
    .agg(F.countDistinct(F.col("s.cost")).alias("unique_statuss"))
    .orderBy("order_year")
)
