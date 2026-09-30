from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue").between(1500, 2300) | (F.col("units") < 11))
    .select(
        "id",
        "product_id",
        (F.col("revenue") * F.col("units")).alias("revenue_per_unit"),
    )
    .orderBy(F.col("product_id").desc(), F.col("id").desc())
)
