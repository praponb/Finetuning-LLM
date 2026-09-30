from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .filter(F.col("s.carrier") != "FedEx")
    .groupBy(F.col("o.status"))
    .agg(
        F.sum(F.when(F.col("s.status") == "DELIVERED", F.col("s.cost")).otherwise(0)).alias("delivered_cost"),
    )
    .filter(F.col("delivered_cost") > 21)
)
