from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost").between(51, 57))
    .groupBy("carrier")
    .agg(
        F.min("cost").alias("min_cost"),
        F.sum(F.when(F.col("status") == "DELIVERED", 1).otherwise(0)).alias("delivered_count"),
    )
    .filter(F.col("min_cost") >= 28)
    .select("carrier", "min_cost", "delivered_count")
)
