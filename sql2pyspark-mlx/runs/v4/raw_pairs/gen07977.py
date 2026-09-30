from pyspark.sql import functions as F

result = (
    df_promotions
    .groupBy("promo_code", F.date_trunc("QUARTER", "starts_on").alias("quarter_start"))
    .agg(
        F.size(F.collect_set("channel")).alias("num_channels"),
        F.count(F.when(F.col("channel") == "store", 1)).alias("store_rows"),
        F.count_if(F.col("promo_code") == "BUNDLE").alias("bundle_events"),
    )
    .orderBy(F.col("bundle_events").desc_nulls_last())
)
