from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") < 4.0)
    .withColumn("rating_filled", F.coalesce("rating", F.lit(0)))
    .withColumn("city_country", F.concat("city", F.lit(" - "), "country"))
    .orderBy(F.col("city").desc(), F.col("city_country").desc())
)
