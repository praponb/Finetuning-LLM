from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .pivot("brand", ["Initech", "Stark"])
    .agg(F.max("stock_qty").alias("value_sum"), F.count(F.lit(1)).alias("item_count"))
)
