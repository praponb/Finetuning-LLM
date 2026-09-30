from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.month("shipped_date").isin([5, 7, 8]))
    .agg(F.sum("cost").alias("total_cost"))
    .orderBy(F.col("total_cost").desc())
)
