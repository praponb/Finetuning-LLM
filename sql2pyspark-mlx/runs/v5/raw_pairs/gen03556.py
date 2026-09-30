from pyspark.sql import functions as F

src = (
    df_suppliers
    .filter(F.col("supplier_name").isNotNull() & ((F.col("rating") > 2.5) | (F.col("country") == "DE")))
    .withColumn("supplier_name_country", F.concat("supplier_name", F.lit("/"), "country"))
)

suppliers_stats = (
    src
    .groupBy(
        F.when(F.col("rating") >= 4.4, "high").when(F.col("rating") >= 2.0, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.sum("rating").alias("total_rating"))
)

result = suppliers_stats.filter(F.col("total_rating") < 20)
