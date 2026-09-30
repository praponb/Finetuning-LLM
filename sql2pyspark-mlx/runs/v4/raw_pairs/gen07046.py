from pyspark.sql import functions as F

base = (
    df_shipments
    .filter(F.col("shipped_date") > F.lit("2023-06-26").cast("date"))
    .withColumn("days_shipped_date_to_delivered_date", F.datediff("delivered_date", "shipped_date"))
)

result = (
    base
    .alias("a")
    .join(df_orders.alias("o"), F.col("a.order_id") == F.col("o.id"), "left")
    .select("a.*", F.col("o.channel"), F.col("o.amount"), F.col("o.shipping_fee"))
)
