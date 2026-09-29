from pyspark.sql import functions as F

flattened = (
    df_carts
    .withColumn("li_item", F.explode("items"))
    .select("id", "customer_id", "coupon_codes", F.col("li_item.promo"))
)

result = (
    flattened
    .groupBy("promo")
    .agg(
        F.count(F.when(F.col("promo") == "VIP", 1)).alias("vip_count"),
        F.count(F.when(F.col("promo") == "WELCOME5", 1)).alias("welcome5_rows"),
        F.sort_array(F.collect_set("promo")).alias("promo_list"),
        F.sort_array(F.collect_set("promo")).alias("promo_list_2"),
        F.count_if(F.col("promo") == "WELCOME5").alias("welcome5_events"),
    )
    .orderBy(F.col("vip_count").asc_nulls_first())
)
