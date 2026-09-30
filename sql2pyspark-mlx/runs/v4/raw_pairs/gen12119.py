from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand") == "Stark")
    .select(
        "id",
        "launch_date",
        "product_name",
        "category",
        F.month("launch_date").alias("launch_date_month"),
        F.round((F.col("stock_qty") / F.col("cost")), 2).alias("stock_qty_per_cost"),
    )
)
