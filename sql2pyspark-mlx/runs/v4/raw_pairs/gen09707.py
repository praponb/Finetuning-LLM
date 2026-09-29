from pyspark.sql import functions as F

filtered_shipments = df_shipments.filter(F.month("shipped_date").isin([1, 8, 12]))

summary = (
    filtered_shipments
    .groupBy("order_id")
    .agg(
        F.max("cost").alias("max_cost"),
        F.sum(F.when(F.col("carrier") == "UPS", 1).otherwise(0)).alias("ups_count"),
    )
)

result = (
    summary
    .filter(F.col("ups_count") < 70)
    .select("order_id", "max_cost")
)
