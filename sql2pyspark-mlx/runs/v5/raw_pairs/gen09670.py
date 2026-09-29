from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        "supplier_id",
        F.date_format("launch_date", "yyyy-MM").alias("launch_month"),
    )
    .agg(
        F.sum(F.when(F.col("brand") == "Initech", F.col("stock_qty")).otherwise(0)).alias("initech_stock_qty"),
        F.sum(F.when(F.col("category") == "Books", F.col("price")).otherwise(0)).alias("books_price"),
    )
    .filter(F.col("initech_stock_qty") > 46)
    .select("supplier_id", "launch_month", "initech_stock_qty", "books_price")
)
