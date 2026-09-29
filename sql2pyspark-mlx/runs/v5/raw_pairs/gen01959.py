from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode("coupon_codes").alias("element"),
    )
)

result = (
    lines
    .groupBy(F.date_trunc("HOUR", "created_at").alias("hour_window"))
    .agg(
        F.size(F.collect_set("element")).alias("num_elements"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
    )
    .orderBy("hour_window")
)
