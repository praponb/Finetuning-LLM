from pyspark.sql import functions as F

flattened = (
    df_carts
    .select("id", "customer_id", F.explode_outer("coupon_codes").alias("element"))
)

result = (
    flattened
    .groupBy("customer_id")
    .agg(
        F.size(F.collect_set("element")).alias("num_elements"),
        F.count(F.when(F.col("element") == "VIP", 1)).alias("vip_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sort_array(F.collect_set("element")).alias("element_list"),
    )
)
