from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(F.year("launch_date").alias("launch_year"))
    .agg(
        F.avg("stock_qty").alias("avg_stock_qty"),
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
    )
    .filter(F.col("avg_stock_qty") >= 170)
)
