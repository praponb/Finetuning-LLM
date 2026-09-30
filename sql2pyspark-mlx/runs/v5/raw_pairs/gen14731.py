from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("value")

lines = (
    df_carts
    .select("*", F.posexplode("items").alias("pos", "elem"))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        (F.col("pos") + 1).alias("item_no"),
        F.col("elem.product_id"),
        F.col("elem.price"),
        F.col("elem.promo"),
    )
)

flattened = lines.select("id", "item_no", F.explode("coupon_codes").alias("value"))

enriched = (
    flattened
    .filter(F.col("item_no") <= 3)
    .select(
        "id",
        "value",
        F.stddev_samp("item_no").over(w_recent).alias("value_stddev_samp_item_no"),
        F.sum("item_no").over(w_recent).alias("value_sum_item_no"),
    )
)

base = (
    enriched
    .filter(F.col("value").isin(["WELCOME5", "VIP"]) & F.col("value").isNotNull())
    .select(
        "*",
        F.round((F.col("value_stddev_samp_item_no") * 1.15), 2).alias("adjusted_value_stddev_samp_item_no"),
    )
)

valid = (
    base
    .filter(F.col("value").isNotNull())
    .withColumn("adjusted_value_stddev_samp_item_no_2", F.round((F.col("value_stddev_samp_item_no") * 100), 2))
)

result = (
    valid
    .withColumn("adjusted_value_stddev_samp_item_no_2_tier", F.when(F.col("adjusted_value_stddev_samp_item_no_2") >= 78, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["value", "adjusted_value_stddev_samp_item_no_2_tier"], ["value"]],
        "value",
        "adjusted_value_stddev_samp_item_no_2_tier",
    )
    .agg(
        F.round(F.sum("value_sum_item_no") / F.nullif(F.count(F.when(F.col("value") == "WELCOME5", 1)), F.lit(0)), 2).alias("value_sum_item_no_per_welcome5"),
        F.percentile_approx("value_sum_item_no", 0.5).alias("value_sum_item_no_p50"),
        F.size(F.collect_set("value")).alias("num_values"),
        F.count_if(F.col("value") == "WELCOME5").alias("welcome5_events"),
        F.sum(F.when(F.col("value") == "BUNDLE", F.col("adjusted_value_stddev_samp_item_no")).otherwise(0)).alias("bundle_adjusted_value_stddev_samp_item_no"),
        F.percentile_approx("value_stddev_samp_item_no", 0.9).alias("value_stddev_samp_item_no_p90"),
    )
    .filter(F.col("value_sum_item_no_p50") < 1000000)
    .orderBy(F.col("value_stddev_samp_item_no_p90").desc())
)
