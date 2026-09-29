from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(F.date_format("shipped_date", "yyyy-MM").alias("shipped_month"))
    .agg(
        F.avg("cost").alias("avg_cost"),
        F.count(F.when(F.col("carrier") == "FedEx", 1)).alias("fedex_count"),
    )
    .filter(F.col("fedex_count") < 4)
    .select("shipped_month", "avg_cost", "fedex_count")
    .orderBy("fedex_count", "shipped_month")
    .limit(3)
)
