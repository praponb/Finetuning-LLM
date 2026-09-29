from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty") <= 324)
    .select(
        "id",
        "cost",
        F.upper("category").alias("category_upper"),
        F.length("brand").alias("brand_length"),
    )
    .orderBy("brand_length")
)
