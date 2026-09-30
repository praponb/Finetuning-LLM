from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([6, 7]))
    .select(
        "id",
        "product_id",
        (F.col("revenue") * F.col("units")).alias("revenue_per_unit"),
        F.date_format("sale_date", "yyyy-MM-dd").alias("sale_date_label"),
    )
    .orderBy("product_id", F.col("id").desc())
)
