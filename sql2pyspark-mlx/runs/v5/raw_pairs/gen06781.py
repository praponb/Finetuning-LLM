from pyspark.sql import functions as F

flattened = df_carts.select("id", F.explode_outer("coupon_codes").alias("tag"))

result = (
    flattened
    .groupBy("tag")
    .agg(
        F.count_if(F.col("tag") == "BUNDLE").alias("bundle_events"),
        F.count_if(F.col("tag") == "WELCOME5").alias("welcome5_events"),
        F.count(F.when(F.col("tag") == "WELCOME5", 1)).alias("welcome5_count"),
    )
    .filter(F.col("bundle_events") < 1000)
)
