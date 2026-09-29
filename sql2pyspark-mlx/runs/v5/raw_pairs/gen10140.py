from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("stock_qty").desc_nulls_first(), "id")

latest = (
    df_products
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    latest
    .groupBy("category", "supplier_id")
    .agg(
        F.corr("stock_qty", "price").alias("corr_stock_qty_price"),
        F.corr("price", "stock_qty").alias("corr_price_stock_qty"),
        F.min("stock_qty").alias("min_stock_qty"),
    )
)
