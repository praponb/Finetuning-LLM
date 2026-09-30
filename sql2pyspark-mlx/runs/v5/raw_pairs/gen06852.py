from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").like("D%"))
    .select(
        "cost",
        "launch_date",
        F.greatest("price", "stock_qty").alias("greatest_price_stock_qty"),
    )
)
