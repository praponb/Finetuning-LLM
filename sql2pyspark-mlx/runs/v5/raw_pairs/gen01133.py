from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft").between(7216, 8122) & (F.col("opened_date") >= "2025-03-01") & (F.lower(F.trim("store_name")) == "mall central"))
    .select(
        "store_name",
        F.when(F.col("region") == "South", 1).otherwise(0).alias("is_south"),
    )
    .orderBy(F.col("is_south").desc())
)
