from pyspark.sql import functions as F

result = (
    df_store_sales
    .select(
        "units",
        "revenue",
        "product_id",
        (F.col("units") + F.col("revenue")).alias("revenue_per_unit"),
    )
    .orderBy("product_id", F.col("revenue_per_unit").desc())
)
