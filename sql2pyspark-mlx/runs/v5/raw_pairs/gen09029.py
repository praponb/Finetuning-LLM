from pyspark.sql import functions as F

cleaned = (
    df_suppliers
    .filter(F.col("city").eqNullSafe(F.lit("New York")))
    .select(
        "id",
        "supplier_name",
        "country",
        "rating",
        F.length("city").alias("city_length"),
    )
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("city_length") >= 70, "HIGH").otherwise("LOW").alias("city_length_tier"),
        F.when(F.col("rating") >= 4.0, "LARGE").otherwise("SMALL").alias("rating_tier"),
    )
    .agg(
        F.round(F.avg("city_length"), 2).alias("avg_city_length"),
        F.count("*").alias("num_rows"),
        F.avg("city_length").alias("avg_city_length_2"),
        F.countDistinct("country").alias("distinct_countries"),
    )
    .orderBy(F.col("avg_city_length_2").desc())
)
