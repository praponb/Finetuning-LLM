from pyspark.sql import functions as F

lines = df_carts.select("id", "customer_id", F.explode_outer("coupon_codes").alias("code"))

result = (
    lines
    .groupBy("customer_id", "code")
    .agg(
        F.count("*").alias("cnt"),
        F.size(F.collect_set("code")).alias("num_codes"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.count("*").alias("cnt_2"),
    )
)
