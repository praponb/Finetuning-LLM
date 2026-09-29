from pyspark.sql import functions as F

filtered_suppliers = (
    df_suppliers
    .filter(F.col("rating") > 2.1)
    .withColumn("supplier_name_filled", F.coalesce("supplier_name", F.lit("N/A")))
)

agg = (
    filtered_suppliers
    .groupBy(
        F.when(F.col("rating") >= 3.8, "high").when(F.col("rating") >= 2.2, "mid").otherwise("low").alias("rating_bucket"),
        "country",
    )
    .agg(F.min("rating").alias("min_rating"))
)

result = agg
