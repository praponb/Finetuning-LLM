from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status") == "RETURNED")
    .groupBy(
        F.month("delivered_date").alias("delivered_month"),
        F.date_trunc("MONTH", "shipped_date").alias("shipped_month"),
    )
    .agg(
        F.max("cost").alias("max_cost"),
        F.sum(F.when(F.col("status") == "LOST", F.col("cost")).otherwise(0)).alias("lost_cost"),
    )
)
