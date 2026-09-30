from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country", "city")
    .agg(F.round(F.avg("rating"), 2).alias("avg_rating"))
)
