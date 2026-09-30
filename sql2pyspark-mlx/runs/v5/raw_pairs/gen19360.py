from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units") != 38)
    .select(
        "id",
        "store_id",
        "product_id",
        (F.col("revenue") * F.col("units")).alias("revenue_per_unit"),
    )
    .orderBy(F.col("id").desc(), F.col("store_id").desc())
    .limit(20)
)
