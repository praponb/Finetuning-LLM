from pyspark.sql import functions as F

filtered = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .withColumn("rating_scaled", F.round((F.col("rating") * 1.07), 2))
    .withColumn("city_filled", F.coalesce("city", F.lit("UNKNOWN")))
)

result = (
    filtered
    .select("id", "country", "rating_scaled", F.col("rating").alias("RATING_VAL"))
    .unpivot(
        ids=["id", "country"],
        values=["rating_scaled", "RATING_VAL"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
