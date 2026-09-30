from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.year("shipped_date") >= 2025)
    .groupBy(F.year("delivered_date").alias("delivered_year"))
    .agg(F.round(F.avg("cost"), 2).alias("avg_cost"))
    .filter(F.col("avg_cost") < 54)
    .orderBy(F.col("delivered_year").desc())
)
