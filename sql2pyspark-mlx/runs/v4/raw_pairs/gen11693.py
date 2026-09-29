from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isNull() | (F.col("cost") >= 76))
    .groupBy("order_id")
    .agg(
        F.sum(F.when(F.col("carrier") == "UPS", F.col("cost")).otherwise(0)).alias("ups_cost"),
    )
)
