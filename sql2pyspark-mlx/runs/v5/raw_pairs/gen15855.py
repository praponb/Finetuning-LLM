from pyspark.sql import functions as F

order_totals = (
    df_shipments
    .filter(F.col("status").isin(["IN_TRANSIT", "LOST", "DELIVERED"]))
    .groupBy("order_id")
    .agg(F.count(F.when(F.col("carrier") == "UPS", 1)).alias("ups_count"))
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("ot"), F.col("o.id") == F.col("ot.order_id"), "inner")
    .filter(F.col("ot.ups_count") > 5)
    .select(F.col("o.id"), F.col("o.status"), F.col("ot.ups_count"))
    .orderBy("id")
)
