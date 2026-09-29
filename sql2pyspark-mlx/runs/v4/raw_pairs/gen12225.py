from pyspark.sql import functions as F

cleaned = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.round((F.col("rating") * 1.1), 2).alias("adjusted_rating"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("city_filled"),
    )
)

result = (
    cleaned
    .groupBy("city")
    .pivot("country", ["US", "DE"])
    .agg(
        F.sum("adjusted_rating").alias("amount"),
        F.count(F.lit(1)).alias("item_count"),
    )
)
