from pyspark.sql import functions as F

result = (
    df_store_sales
    .select(
        "id",
        "units",
        "product_id",
        "sale_date",
        F.col("revenue").cast("int").alias("revenue_int"),
    )
    .orderBy(F.col("units").desc())
)
