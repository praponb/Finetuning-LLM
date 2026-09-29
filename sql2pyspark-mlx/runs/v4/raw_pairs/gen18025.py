from pyspark.sql import functions as F

recent_suppliers = (
    df_suppliers
    .filter((F.lower(F.trim("supplier_name")) == "tokyo traders") & (F.col("country") == "JP"))
    .withColumn("country_supplier_name", F.concat("country", F.lit(" - "), "supplier_name"))
)

agg = (
    recent_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.5, "high").when(F.col("rating") >= 2.4, "mid").otherwise("low").alias("rating_bucket"),
        "country",
    )
    .agg(
        F.avg("rating").alias("avg_rating"),
        F.count("rating").alias("rating_count"),
    )
)

result = agg.filter(F.col("avg_rating") > 2.7)
