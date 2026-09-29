from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.explode("attributes").alias("k", "value"),
    )
)

result = (
    lines
    .groupBy("customer_id", "k")
    .agg(
        F.count(F.when(F.col("k") == "utm_source", 1)).alias("utm_source_rows"),
        F.size(F.collect_set("k")).alias("num_ks"),
        F.sort_array(F.collect_set("k")).alias("k_list"),
        F.sort_array(F.collect_set("k")).alias("k_list_2"),
        F.size(F.collect_set("k")).alias("num_ks_2"),
        F.sort_array(F.collect_set("k")).alias("k_list_3"),
    )
    .orderBy(F.col("num_ks_2").desc_nulls_last())
)
