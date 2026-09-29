from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").like("%Bottle%"))
    .select("id", "launch_date", "stock_qty")
    .unionByName(
        df_products
        .filter(F.col("product_name").like("%USB%"))
        .select("id", "launch_date", "stock_qty")
    )
)
