from pyspark.sql import functions as F

exploded = df_carts.select("id", "customer_id", F.explode("attributes").alias("key", "v"))

result = (
    exploded
    .groupingSets([["customer_id", "key"], ["customer_id"], []], "customer_id", "key")
    .agg(
        F.count(F.when(F.col("key") == "gift", 1)).alias("gift_rows"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.sort_array(F.collect_set("key")).alias("key_list"),
        F.count(F.when(F.col("key") == "utm_source", 1)).alias("utm_source_count"),
        F.countDistinct("customer_id").alias("distinct_customer_ids_2"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("distinct_customer_ids_2").desc())
)
