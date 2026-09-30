from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(
        F.date_trunc("MONTH", "order_date").alias("order_month"),
        F.when(F.col("shipping_fee") >= 30, "high").when(F.col("shipping_fee") >= 11, "mid").otherwise("low").alias("shipping_fee_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("channel") == "web", F.col("discount")).otherwise(0)).alias("web_discount"),
        F.countDistinct("amount").alias("unique_stores"),
    )
)
