from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units").isNotNull())
    .select(
        "id",
        "revenue",
        F.round((F.col("revenue") * 1.15), 2).alias("revenue_adjusted"),
        F.floor("units").alias("units_whole"),
    )
    .orderBy(F.col("id").desc())
)
