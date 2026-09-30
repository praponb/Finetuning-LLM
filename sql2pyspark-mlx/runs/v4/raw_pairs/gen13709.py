from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(~(F.col("store_id").isin([1, 2, 4]) & F.col("product_id").isin([3, 5])))
    .select("id", "units", F.last_day("sale_date").alias("sale_date_month_end"))
    .orderBy("id")
)
