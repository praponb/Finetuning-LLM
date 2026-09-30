from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category", "brand")
    .agg(
        F.sum(F.when(F.col("brand") == "Globex", F.col("stock_qty")).otherwise(0)).alias("globex_stock_qty"),
        F.min("launch_date").alias("first_launch_date"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("globex_stock_qty") < 79)
)
