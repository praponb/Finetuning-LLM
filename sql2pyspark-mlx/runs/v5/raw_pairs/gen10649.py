from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "code")
shipping_fee_by_customer_id = df_orders.groupBy("customer_id").agg(F.sum("shipping_fee").alias("sum_order_shipping_fee"))

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.explode("attributes").alias("attr_key", "value"),
    )
)

exploded_carts = (
    exploded
    .select(
        "id",
        "customer_id",
        "attr_key",
        F.explode_outer("coupon_codes").alias("code"),
    )
)

enriched = (
    exploded_carts
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id").eqNullSafe(F.col("c.id")), "left")
    .select("s.*", F.col("c.name"))
)

enriched_carts = (
    enriched
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .select("a.id", "a.customer_id", "a.code", "a.name", F.col("c.age"))
)

metrics = enriched_carts.select("*", F.min("age").over(w).alias("customer_id_min_age"))

daily = (
    metrics
    .cube("customer_id")
    .agg(
        F.avg(F.when(F.col("code") == "BUNDLE", F.col("age"))).alias("bundle_avg_age"),
        F.count_if(F.col("code") == "VIP").alias("vip_events"),
        F.countDistinct("id").alias("distinct_ids"),
        F.avg(F.when(F.col("code") == "BUNDLE", F.col("customer_id_min_age"))).alias("bundle_avg_customer_id_min_age"),
        F.count_if(F.col("code") == "WELCOME5").alias("welcome5_events"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.grouping("customer_id").alias("grouping_id"),
    )
    .filter(F.col("bundle_avg_age") < 1000000)
)

result = (
    daily
    .join(shipping_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "customer_id",
        "bundle_avg_age",
        "vip_events",
        "distinct_ids",
        "distinct_customer_ids",
        "grouping_id",
        F.col("sum_order_shipping_fee"),
    )
    .orderBy(F.col("bundle_avg_age").desc_nulls_last())
)
