from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .agg(
        F.sum(F.when(F.col("brand") == "Umbrella", F.col("stock_qty")).otherwise(0)).alias("umbrella_stock_qty"),
    )
    .orderBy("category")
)
