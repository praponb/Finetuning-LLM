from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue").between(2300, 3300) & (F.col("product_id") == 8) & (F.col("units") == 23))
    .select(
        "id",
        "product_id",
        F.date_format("sale_date", "yyyy-MM-dd").alias("sale_date_label"),
    )
    .orderBy(F.col("product_id").desc())
)
