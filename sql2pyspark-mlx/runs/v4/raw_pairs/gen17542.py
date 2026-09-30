from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") >= 3.4)
    .select("id", "city", "country", F.round("rating", 1).alias("rating_rounded"))
    .orderBy(F.col("rating_rounded").desc())
)
