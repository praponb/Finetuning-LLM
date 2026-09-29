from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("stock_qty") > 110) & (F.col("stock_qty") < 362))
    .select(
        "cost",
        F.concat("category", F.lit(" "), "product_name").alias("category_product_name"),
    )
)
