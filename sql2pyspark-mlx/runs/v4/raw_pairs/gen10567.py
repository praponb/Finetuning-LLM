from pyspark.sql import functions as F

result = (
    df_shipments
    .alias("a")
    .join(df_orders.alias("o"), F.col("a.order_id") == F.col("o.id"), "left")
    .select("a.*", F.col("o.discount"), F.col("o.order_date"))
)
