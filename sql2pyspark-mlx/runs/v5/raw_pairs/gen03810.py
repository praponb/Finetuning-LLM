from pyspark.sql import functions as F

result = (
    df_suppliers
    .select(
        "id",
        "country",
        "city",
        F.when(F.col("country") == "US", 1).otherwise(0).alias("is_us"),
        F.when(F.col("city") == "Berlin", 1).otherwise(0).alias("is_berlin"),
    )
)
