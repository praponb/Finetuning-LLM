from pyspark.sql import functions as F

result = (
    df_store_sales
    .select(
        "id",
        "sale_date",
        "product_id",
        "store_id",
        (F.col("revenue") + F.col("units")).alias("revenue_per_unit"),
    )
    .orderBy(F.col("revenue_per_unit").desc())
)
