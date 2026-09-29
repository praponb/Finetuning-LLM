from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.upper(F.trim("supplier_name")) == "EUROGOODS GMBH")
    .groupBy("country")
    .agg(
        F.sum(F.when(F.col("city") == "Bangkok", 1).otherwise(0)).alias("bangkok_count"),
        F.min("rating").alias("min_rating"),
        F.max("rating").alias("max_rating"),
    )
    .filter(F.col("bangkok_count") < 46)
    .orderBy(F.col("max_rating").desc())
)
