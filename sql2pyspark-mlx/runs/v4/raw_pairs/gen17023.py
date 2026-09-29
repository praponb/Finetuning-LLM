from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft").between(12084, 12835) & (F.col("sqft") >= 6655) & (F.col("region") == "West"))
    .select(
        "id",
        "sqft",
        F.round((F.col("sqft") * 1.1), 2).alias("sqft_scaled"),
        F.length("store_name").alias("store_name_length"),
    )
)
