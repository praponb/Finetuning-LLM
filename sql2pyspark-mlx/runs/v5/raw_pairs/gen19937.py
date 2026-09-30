from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("coupon_codes").alias("code"),
    )
)

result = (
    lines
    .groupBy("customer_id")
    .agg(
        F.sort_array(F.collect_set("code")).alias("code_list"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("code") == "WELCOME5", 1)).alias("welcome5_rows"),
        F.count("*").alias("num_rows"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("duration_min"),
    )
    .orderBy(F.col("duration_min").asc_nulls_first())
)
