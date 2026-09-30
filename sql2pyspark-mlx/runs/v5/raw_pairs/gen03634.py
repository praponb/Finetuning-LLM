from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel") == "store")
    .groupBy(F.year("order_date").alias("order_year"))
    .agg(
        F.max("discount").alias("max_discount"),
        F.sum("discount").alias("total_discount"),
    )
    .filter(F.col("max_discount") > 0.39)
    .select("order_year", "max_discount", "total_discount")
    .orderBy(F.col("order_year").desc())
)
