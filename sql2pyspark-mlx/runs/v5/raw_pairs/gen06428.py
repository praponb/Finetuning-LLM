from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating").between(2.2, 4.1))
    .groupBy("city", "country")
    .agg(F.round(F.avg("rating"), 2).alias("avg_rating"))
)
