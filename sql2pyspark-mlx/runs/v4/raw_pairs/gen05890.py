from pyspark.sql import functions as F

result = (
    df_suppliers
    .select(
        "id",
        "city",
        "country",
        "rating",
        F.when(F.col("city") == "Singapore", 1).otherwise(0).alias("is_singapore"),
        F.coalesce("city", F.lit("N/A")).alias("city_filled"),
    )
)
