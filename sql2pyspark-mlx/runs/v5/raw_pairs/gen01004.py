from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("city")
    .agg(
        F.sum("rating").alias("total_rating"),
        F.avg("rating").alias("avg_rating"),
    )
)
