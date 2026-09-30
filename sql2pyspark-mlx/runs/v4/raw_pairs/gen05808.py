from pyspark.sql import functions as F

result = (
    df_products
    .select(
        "id",
        "cost",
        "brand",
        "tags",
        F.col("price").cast("int").alias("price_int"),
    )
)
