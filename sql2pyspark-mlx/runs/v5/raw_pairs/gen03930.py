from pyspark.sql import functions as F

result = (
    df_products
    .filter(~F.col("category").isin(["Grocery", "Home"]) & (F.col("cost") >= 450))
    .groupBy(F.date_trunc("MONTH", "launch_date").alias("launch_month"), "brand")
    .agg(F.sum("stock_qty").alias("total_stock_qty"))
    .filter(F.col("total_stock_qty") >= 1600)
    .orderBy("launch_month", "brand")
    .limit(5)
)
