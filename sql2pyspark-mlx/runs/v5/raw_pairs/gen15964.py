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
        F.count_if(F.col("element") == "SPRING10").alias("spring10_events"),
        F.size(F.collect_set("element")).alias("num_elements_2"),
    )
    .filter(F.col("num_elements_2") > 1)
    .orderBy(F.col("customer_id").desc())
)
