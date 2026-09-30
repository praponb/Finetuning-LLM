from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.month("shipped_date").isin([1, 3, 7]))
    .groupBy("status")
    .agg(
        F.count("*").alias("cnt"),
        F.sum(F.when(F.col("status") == "DELIVERED", F.col("cost")).otherwise(0)).alias("delivered_cost"),
    )
    .filter(F.col("delivered_cost") >= 62)
    .select("status", "cnt", "delivered_cost")
    .orderBy(F.col("delivered_cost").desc())
)
