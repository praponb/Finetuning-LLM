from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty") != 315)
    .select("stock_qty", "brand")
    .unionByName(
        df_products
        .filter(F.col("price").between(380, 450))
        .select("stock_qty", "brand")
    )
    .distinct()
)
