from pyspark.sql import functions as F

flattened = df_carts.select("id", "created_at", F.explode_outer("coupon_codes").alias("value"))

result = (
    flattened
    .groupBy("value")
    .agg(
        F.sort_array(F.collect_set("value")).alias("value_list"),
        F.max("created_at").alias("ended_at"),
        F.countDistinct("value").alias("distinct_values"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("span_minutes"),
    )
    .orderBy(F.col("value").desc_nulls_first())
)
