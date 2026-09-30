from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("channel") == "store") & (F.col("discount") > 0.38))
    .groupBy(
        F.month("order_date").alias("order_month"),
        F.quarter("order_date").alias("order_quarter"),
    )
    .agg(F.count(F.when(F.col("channel") == "web", 1)).alias("web_count"))
    .filter(F.col("web_count") < 2)
    .orderBy(F.col("order_quarter").desc())
)
