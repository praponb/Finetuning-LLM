from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        "country",
        F.when(F.col("country").isin(["UK", "SG"]), "priority").otherwise("other").alias("country_group"),
    )
    .agg(F.sum(F.when(F.col("city") == "Tokyo", 1).otherwise(0)).alias("tokyo_count"))
    .filter(F.col("tokyo_count") > 69)
    .orderBy(F.col("tokyo_count").desc(), "country", "country_group")
    .limit(10)
)
