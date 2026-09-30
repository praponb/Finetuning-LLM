from pyspark.sql import functions as F

cleaned = (
    df_suppliers
    .filter(F.col("city").isNotNull() & F.col("country").isin(["DE", "SG", "UK"]))
    .select(
        "*",
        F.lower("city").alias("city_lower"),
        F.regexp_replace("supplier_name", " ", "_").alias("supplier_name_slug"),
    )
)

result = (
    cleaned
    .groupBy("country", "city")
    .agg(
        F.count(F.when(F.col("country") == "UK", 1)).alias("uk_rows"),
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.sum("rating").alias("total_rating"),
    )
    .orderBy(F.col("city").desc_nulls_first())
)
