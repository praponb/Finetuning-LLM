from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("order_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("carrier") == "UPS", 1).otherwise(0))) / F.count("*")), 1).alias("pct_ups"),
        F.sum(F.when(F.col("carrier") == "Kerry", F.col("cost")).otherwise(0)).alias("kerry_cost"),
    )
)
