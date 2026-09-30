from pyspark.sql import functions as F

src = (
    df_orders
    .filter(~(((F.col("shipping_fee") >= 13) & (F.col("amount") < 1330)) | (F.col("customer_id") == 2)))
    .withColumn("status_lower", F.lower("status"))
)

agg = (
    src
    .groupBy("customer_id")
    .agg(
        F.avg("discount").alias("avg_discount"),
        F.count("amount").alias("amount_count"),
    )
)

result = (
    agg
    .select("customer_id", "avg_discount", "amount_count")
    .orderBy(F.col("amount_count").desc())
)
