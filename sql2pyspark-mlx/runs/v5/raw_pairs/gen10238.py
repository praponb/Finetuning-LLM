from pyspark.sql import functions as F

result = (
    df_store_sales
    .agg(
        F.avg("revenue").alias("avg_revenue"),
        F.countDistinct("units").alias("unique_stores"),
    )
    .orderBy(F.col("unique_stores").desc(), F.col("avg_revenue").desc())
)
