from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.orderBy(F.col("is_utm_source_num").desc())

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.explode_outer("attributes").alias("attr_key", "value"),
    )
)

exploded = flattened.select("attr_key", "value", F.explode("coupon_codes").alias("code"))

valid = (
    exploded
    .filter(F.col("attr_key").isNotNull() & (F.col("code") == "VIP"))
    .select(
        "*",
        F.when(F.col("attr_key") == "utm_source", 1).otherwise(0).alias("is_utm_source"),
        F.initcap(F.trim("attr_key")).alias("attr_key_title"),
    )
)

base = (
    valid
    .filter(F.col("attr_key").isNotNull())
    .select(
        "attr_key",
        "value",
        "code",
        "is_utm_source",
        "attr_key_title",
        F.lower("code").alias("code_lower"),
        F.coalesce("code", F.lit("OTHER")).alias("resolved_code"),
    )
)

filtered = (
    base
    .filter(F.col("code").isNotNull() & (F.col("attr_key") != "device"))
    .withColumn("code_attr_key", F.concat("code", F.lit(", "), "attr_key"))
    .withColumn("is_utm_source_num", F.col("is_utm_source").cast("double"))
    .withColumn("attr_key_filled", F.coalesce("attr_key", F.lit("NONE")))
)

scored = (
    filtered
    .select(
        "attr_key",
        "is_utm_source",
        "is_utm_source_num",
        F.percent_rank().over(w_part).alias("is_utm_source_num_percent_rank"),
    )
)

valid_carts = (
    scored
    .filter(F.col("attr_key") != "device")
    .select(
        "*",
        F.greatest("is_utm_source_num_percent_rank", "is_utm_source_num").alias("greatest_is_utm_source_num_percent_rank_is_utm_source_num"),
        F.col("is_utm_source_num_percent_rank").cast("int").alias("is_utm_source_num_percent_rank_int"),
    )
)

result = (
    valid_carts
    .groupBy("attr_key")
    .agg(
        F.countDistinct("attr_key").alias("distinct_attr_keys"),
        F.max("is_utm_source_num").alias("max_is_utm_source_num"),
        F.sum("greatest_is_utm_source_num_percent_rank_is_utm_source_num").alias("total_greatest_is_utm_source_num_percent_rank_is_utm_source_num"),
        F.count(F.when(F.col("attr_key") == "gift", 1)).alias("gift_rows"),
        F.max(F.when(F.col("attr_key") == "gift", F.col("is_utm_source_num_percent_rank_int"))).alias("gift_max_is_utm_source_num_percent_rank_int"),
    )
    .filter(F.col("gift_rows") >= 1)
)
