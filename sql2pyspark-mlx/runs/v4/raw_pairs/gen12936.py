from pyspark.sql import functions as F

exploded = df_carts.select("id", "customer_id", F.explode_outer("coupon_codes").alias("tag"))

base = (
    exploded
    .filter(F.col("tag") == "WELCOME5")
    .select("*", F.upper("tag").alias("tag_upper"))
)

joined = (
    base
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .select("s.*", F.col("c.segment"))
)

valid = (
    joined
    .filter(F.col("customer_id") == 4)
    .select(
        "id",
        "customer_id",
        "tag",
        "segment",
        F.coalesce("segment", F.lit("N/A")).alias("segment_filled"),
        F.coalesce("segment", F.lit("unassigned")).alias("segment_filled_2"),
        F.coalesce("tag", F.lit("DIRECT")).alias("resolved_tag"),
    )
)

result = (
    valid
    .groupBy("segment")
    .agg(
        F.count_if(F.col("tag") == "BUNDLE").alias("bundle_events"),
        F.count_if(F.col("segment") == "Corporate").alias("corporate_events"),
        F.count("*").alias("cnt"),
        F.size(F.collect_set("segment")).alias("num_segments"),
        F.count_if(F.col("tag") == "WELCOME5").alias("welcome5_events"),
        F.sort_array(F.collect_set("tag")).alias("tag_list"),
    )
)
