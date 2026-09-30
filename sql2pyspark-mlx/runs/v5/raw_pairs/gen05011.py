from pyspark.sql import functions as F

filtered = (
    df_products
    .filter(F.col("launch_date") >= F.lit("2023-05-05").cast("date"))
    .select("*", F.least("cost", "stock_qty").alias("least_cost_stock_qty"))
)

result = (
    filtered
    .select("category")
    .intersectAll(
        df_products
        .filter(F.col("stock_qty") < 334)
        .select("category")
    )
)
