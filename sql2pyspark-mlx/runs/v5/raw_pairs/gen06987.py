from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w_grp = Window.partitionBy("code").orderBy(F.col("is_welcome5").asc_nulls_first())
w3 = Window.partitionBy("code")

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("attr_key", "attr_value"),
    )
)

lines_carts = (
    lines
    .select("id", "created_at", "attr_value", F.explode("coupon_codes").alias("code"))
)

filtered = (
    lines_carts
    .filter(F.col("code").eqNullSafe(F.lit("VIP")))
    .withColumn("is_welcome5", F.when(F.col("code") == "WELCOME5", 1).otherwise(0))
)

with_windows = (
    filtered
    .select(
        "*",
        F.max("is_welcome5").over(w).alias("is_welcome5_max_1h"),
        F.rank().over(w_grp).alias("is_welcome5_rank"),
        F.stddev_samp("is_welcome5").over(w3).alias("code_stddev_samp_is_welcome5"),
    )
)

valid = (
    with_windows
    .filter(~F.col("code").eqNullSafe(F.lit("VIP")))
    .select(
        "id",
        "code",
        "is_welcome5_max_1h",
        "code_stddev_samp_is_welcome5",
        F.when(F.col("is_welcome5_max_1h") >= 64, "High").when(F.col("is_welcome5_max_1h") >= 24, "Medium").otherwise("Low").alias("is_welcome5_max_1h_tier"),
        F.lower("code").alias("code_lower"),
        F.coalesce("code", F.lit("NONE")).alias("code_clean"),
    )
)

summary = (
    valid
    .withColumn("code_stddev_samp_is_welcome5_tier", F.when(F.col("code_stddev_samp_is_welcome5") >= 86, "LARGE").otherwise("SMALL"))
    .rollup("code_stddev_samp_is_welcome5_tier", "code")
    .agg(
        F.sort_array(F.collect_set("code")).alias("code_list"),
        F.avg(F.when(F.col("code") == "VIP", F.col("code_stddev_samp_is_welcome5"))).alias("vip_avg_code_stddev_samp_is_welcome5"),
        F.grouping("code_stddev_samp_is_welcome5_tier").alias("grouping_level"),
    )
)

result = (
    summary
    .select(
        "code_stddev_samp_is_welcome5_tier",
        "code_list",
        "vip_avg_code_stddev_samp_is_welcome5",
    )
    .orderBy(F.col("code_stddev_samp_is_welcome5_tier").asc_nulls_last())
)
