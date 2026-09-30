from pyspark.sql import functions as F

base = df_suppliers.filter(F.col("supplier_name").like("%Lio%"))

agg = (
    base
    .groupBy("city")
    .agg(F.round(F.avg("rating"), 2).alias("avg_rating"))
)

result = (
    agg
    .filter(F.col("avg_rating") >= 27)
    .orderBy(F.col("city").desc())
    .limit(5)
)
