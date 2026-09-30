from pyspark.sql import functions as F

result = (
    df_store_sales
    .select(
        "id",
        "sale_date",
        "store_id",
        "product_id",
        (F.col("revenue") - F.col("units")).alias("revenue_per_unit"),
    )
    .orderBy(F.col("product_id").desc())
)
