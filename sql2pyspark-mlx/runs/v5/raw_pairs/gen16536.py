from pyspark.sql import functions as F

base = df_products.filter((F.col("stock_qty") < 252) & (F.col("stock_qty") >= 191))

summary = (
    base
    .groupBy(F.date_trunc("MONTH", "launch_date").alias("launch_month"), "category")
    .agg(
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
        F.min("stock_qty").alias("min_stock_qty"),
    )
)

result = summary.select("launch_month", "category", "brand_list", "min_stock_qty")
