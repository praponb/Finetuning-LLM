from pyspark.sql import functions as F

valid = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .withColumn("supplier_name_upper", F.upper("supplier_name"))
    .withColumn("resolved_city", F.coalesce("city", F.lit("UNKNOWN")))
)

result = (
    valid
    .groupBy(
        F.when(F.col("rating") >= 4.1, "HIGH").otherwise("LOW").alias("rating_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.sum(F.when(F.col("country") == "US", F.col("rating")).otherwise(0)).alias("us_rating"),
        F.round(F.sum("rating") / F.nullif(F.count(F.when(F.col("city") == "Berlin", 1)), F.lit(0)), 2).alias("rating_per_berlin"),
    )
    .orderBy(F.col("rating_tier").desc())
)
