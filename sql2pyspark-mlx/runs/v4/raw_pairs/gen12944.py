from pyspark.sql import functions as F

result = (
    df_suppliers
    .agg(
        F.max("rating").alias("max_rating"),
        F.round(F.avg("rating"), 2).alias("avg_rating"),
    )
    .orderBy("avg_rating")
)
