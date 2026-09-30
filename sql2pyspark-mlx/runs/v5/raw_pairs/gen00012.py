from pyspark.sql import functions as F

result = (
    df_products
    .filter(~(F.col("supplier_id").isin([13, 27, 29, 37]) & (F.col("launch_date") >= "2024-05-01") & F.col("cost").between(410, 690)))
    .select(
        "product_name",
        "tags",
        F.substring("product_name", 1, 3).alias("product_name_prefix"),
    )
    .orderBy("product_name")
)
