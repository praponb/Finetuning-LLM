from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty") != 354)
    .select("id", "brand", "stock_qty")
    .intersect(
        df_products
        .filter(F.col("brand") == "Globex")
        .select("id", "brand", "stock_qty")
    )
    .orderBy(F.col("brand").desc())
)
