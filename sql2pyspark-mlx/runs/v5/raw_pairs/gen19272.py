from pyspark.sql import functions as F

result = (
    df_products
    .select(F.lit("product").alias("origin"), "cost")
    .unionByName(
        df_store_sales
        .select(F.lit("store_sale").alias("origin"), F.col("revenue").alias("cost"))
    )
)
