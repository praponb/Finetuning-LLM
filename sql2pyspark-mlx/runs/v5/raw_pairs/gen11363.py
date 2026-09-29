from pyspark.sql import functions as F

exploded = df_carts.select("id", "customer_id", "coupon_codes", F.inline("items"))

result = (
    exploded
    .groupBy("product_id")
    .agg(
        F.size(F.collect_set("promo")).alias("num_promos"),
        F.count(F.when(F.col("promo") == "WELCOME5", 1)).alias("welcome5_count"),
        F.sort_array(F.collect_set("promo")).alias("promo_list"),
        F.count("*").alias("num_rows"),
    )
    .filter(F.col("num_rows") > 1)
    .orderBy(F.col("product_id").asc_nulls_last())
)
