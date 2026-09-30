from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").isNotNull())
    .select("store_name", "sqft")
    .subtract(
        df_stores
        .filter(F.col("city") != "Tokyo")
        .select("store_name", "sqft")
    )
)
