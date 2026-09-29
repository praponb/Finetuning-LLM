from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status") == "LOST")
    .groupBy(F.year("shipped_date").alias("shipped_year"))
    .agg(F.avg("cost").alias("avg_cost"))
)
