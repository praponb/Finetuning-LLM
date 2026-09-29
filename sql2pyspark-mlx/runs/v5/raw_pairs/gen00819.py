from pyspark.sql import functions as F

flattened = df_carts.select("id", F.explode_outer("coupon_codes").alias("value"))

result = (
    flattened
    .groupBy("value")
    .agg(
        F.count_if(F.col("value") == "BUNDLE").alias("bundle_events"),
        F.sort_array(F.collect_set("value")).alias("value_list"),
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("value") == "SPRING10", 1)).alias("spring10_count"),
    )
)
