from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("k", "value"),
    )
)

result = (
    lines
    .groupBy("k", F.date_trunc("DAY", "created_at").alias("activity_day"))
    .agg(
        F.sort_array(F.collect_set("k")).alias("k_list"),
        F.min("created_at").alias("first_seen"),
        F.sort_array(F.collect_set("k")).alias("k_list_2"),
    )
    .orderBy(F.col("activity_day").desc_nulls_first())
)
