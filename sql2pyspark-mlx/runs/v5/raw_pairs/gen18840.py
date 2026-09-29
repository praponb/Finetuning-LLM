from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.length("product_name") > 10)
    .select("id", "product_name")
    .unionByName(
        df_products
        .filter(F.col("stock_qty") > 207)
        .select("id", "product_name")
    )
    .distinct()
)
