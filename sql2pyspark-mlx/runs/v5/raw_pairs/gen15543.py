from pyspark.sql import functions as F

exploded = df_carts.select("id", "customer_id", F.explode_outer("coupon_codes").alias("code"))

result = (
    exploded
    .groupBy("customer_id")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
    )
)
