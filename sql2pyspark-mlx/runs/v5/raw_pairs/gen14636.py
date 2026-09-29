from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units") != 24)
    .select(
        "id",
        "store_id",
        "product_id",
        F.col("revenue").cast("int").alias("revenue_int"),
    )
)
