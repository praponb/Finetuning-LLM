from pyspark.sql import functions as F

result = (
    df_suppliers
    .agg(F.avg("rating").alias("avg_rating"))
    .orderBy("avg_rating")
)
