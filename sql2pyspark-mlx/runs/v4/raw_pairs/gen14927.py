from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("category") == "Electronics") & F.col("launch_date").between("2023-09-01", "2025-07-01"))
    .groupBy("brand")
    .agg(
        F.min("cost").alias("min_cost"),
        F.sum("stock_qty").alias("total_stock_qty"),
    )
    .filter(F.col("total_stock_qty") >= 700)
    .drop("total_stock_qty")
)
