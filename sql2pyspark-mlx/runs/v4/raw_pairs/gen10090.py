from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("country").isin(["UK", "SG"]), "core").otherwise("other").alias("country_group"),
    )
    .agg(F.sum(F.when(F.col("city") == "Berlin", 1).otherwise(0)).alias("berlin_count"))
    .filter(F.col("berlin_count") < 77)
    .orderBy(F.col("country_group").desc())
)
