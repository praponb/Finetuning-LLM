from pyspark.sql import functions as F

valid_products = df_products.filter(~F.col("brand").isin(["Globex", "Stark"]))

agg = (
    valid_products
    .groupBy("category", F.date_trunc("MONTH", "launch_date").alias("launch_month"))
    .agg(
        F.max("launch_date").alias("latest_launch_date"),
        F.countDistinct("stock_qty").alias("unique_suppliers"),
    )
)

result = agg.filter(F.col("unique_suppliers") < 5)
