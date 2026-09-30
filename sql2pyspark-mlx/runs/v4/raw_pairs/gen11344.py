from pyspark.sql import functions as F

base = df_suppliers.filter(F.col("city").isNull())

agg = (
    base
    .groupBy("country")
    .agg(F.max("rating").alias("max_rating"))
)

result = (
    agg
    .filter(F.col("max_rating") >= 3.4)
    .select("country", "max_rating")
    .orderBy(F.col("max_rating").desc())
)
