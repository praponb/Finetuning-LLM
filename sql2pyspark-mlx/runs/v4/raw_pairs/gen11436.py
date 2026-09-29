from pyspark.sql import functions as F

src = df_orders.filter(F.col("channel") != "web")

summary = (
    src
    .groupBy(F.year("order_date").alias("order_year"))
    .agg(F.count("discount").alias("discount_count"))
)

result = (
    summary
    .filter(F.col("discount_count") > 5)
    .select("order_year", "discount_count")
    .orderBy(F.col("discount_count").desc())
)
