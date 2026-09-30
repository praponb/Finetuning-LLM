from pyspark.sql import functions as F

result = (
    df_products
    .filter(~F.col("category").isin(["Home", "Sports", "Grocery"]))
    .groupBy(
        F.date_trunc("MONTH", "launch_date").alias("launch_month"),
        "category",
        "brand",
    )
    .agg(F.max("price").alias("max_price"))
    .filter(F.col("max_price") > 720)
)
