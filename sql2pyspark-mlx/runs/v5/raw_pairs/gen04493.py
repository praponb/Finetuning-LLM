from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isin(["West", "North"]) & F.col("city").isin(["London", "Bangkok", "Tokyo"]) & (F.col("sqft") > 9722))
    .select(
        "id",
        "city",
        "region",
        "store_name",
        F.upper("store_name").alias("store_name_upper"),
    )
)
