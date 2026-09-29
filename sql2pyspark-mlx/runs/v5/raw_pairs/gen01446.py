from pyspark.sql import functions as F

cleaned = (
    df_store_sales
    .filter(F.col("revenue").isNull())
    .select("*", F.floor("units").alias("units_whole"))
)

result = (
    cleaned
    .select("product_id")
    .intersectAll(
        df_store_sales
        .filter(F.col("units").between(28, 34))
        .select("product_id")
    )
)
