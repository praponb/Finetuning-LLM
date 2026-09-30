from pyspark.sql import functions as F

lines = (
    df_carts
    .select("id", "customer_id", F.explode_outer("coupon_codes").alias("element"))
)

result = (
    lines
    .groupBy("customer_id", "element")
    .agg(
        F.sort_array(F.collect_set("element")).alias("element_list"),
        F.count(F.when(F.col("element") == "SPRING10", 1)).alias("spring10_count"),
        F.size(F.collect_set("element")).alias("num_elements"),
    )
)
