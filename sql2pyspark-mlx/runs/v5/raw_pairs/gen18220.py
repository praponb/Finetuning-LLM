from pyspark.sql import functions as F

base = (
    df_orders
    .filter(((F.col("amount") >= 720) & F.col("amount").between(520, 1010)) | (F.col("shipping_fee") >= 17))
    .withColumn("least_amount_shipping_fee", F.least("amount", "shipping_fee"))
)

agg = (
    base
    .groupBy("channel")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("channel") == "web", 1).otherwise(0))) / F.count("*")), 1).alias("pct_web"),
        F.min("order_date").alias("min_order_date"),
    )
)

result = (
    agg
    .filter(F.col("pct_web") > 47)
    .select("channel", "pct_web")
)
