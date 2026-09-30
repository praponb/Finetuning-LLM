from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("amount") <= 520) & F.col("channel").isin(["store", "web"]))
    .withColumn("order_date_label", F.date_format("order_date", "yyyy-MM-dd"))
    .orderBy(F.col("id").desc())
)
