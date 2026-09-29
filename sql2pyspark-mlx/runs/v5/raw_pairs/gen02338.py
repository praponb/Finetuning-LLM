from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("store_name").isNotNull() & (F.col("sqft") >= 6692)) | (F.col("city") != "Bangkok"))
    .select(
        "id",
        "store_name",
        "sqft",
        F.when(F.col("city") == "Tokyo", 1).otherwise(0).alias("is_tokyo"),
        F.length("store_name").alias("store_name_length"),
    )
)
