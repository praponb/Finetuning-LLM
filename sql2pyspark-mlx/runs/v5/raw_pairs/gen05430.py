from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty") < 336)
    .select("id", "product_name", "stock_qty")
    .subtract(
        df_products
        .filter(F.col("product_name").isNotNull())
        .select("id", "product_name", "stock_qty")
    )
    .orderBy("product_name")
)
