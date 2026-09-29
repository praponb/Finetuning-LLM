from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment", "customer_id").orderBy("age")
w2 = Window.partitionBy("customer_id")

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode("coupon_codes").alias("element"),
    )
)

with_dims = (
    lines
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "inner")
    .select("e.id", "e.customer_id", "e.element", F.col("b.email"))
)

joined = (
    with_dims
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.segment"))
)

valid = (
    joined
    .filter(F.col("customer_id").isNotNull())
    .withColumn("segment_email_key", F.concat_ws("-", "segment", "email"))
)

enriched = (
    valid
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "inner")
    .filter(F.col("d.city") == "London")
    .select(
        "a.customer_id",
        "a.segment",
        "a.segment_email_key",
        F.col("d.city"),
        F.col("d.age"),
        F.col("d.name"),
    )
)

enriched_carts = (
    enriched
    .select(
        "segment",
        "segment_email_key",
        "name",
        F.percent_rank().over(w).alias("age_percent_rank"),
        F.round((F.col("age") / F.sum("age").over(w2)), 4).alias("age_share"),
    )
)

valid_carts = (
    enriched_carts
    .filter(F.col("segment").isNotNull())
    .select(
        "segment",
        "segment_email_key",
        "name",
        "age_percent_rank",
        "age_share",
        F.lower("name").alias("name_lower"),
    )
)

result = (
    valid_carts
    .groupBy(
        F.when(F.col("age_share") >= 0.7, "HIGH").otherwise("LOW").alias("age_share_tier"),
    )
    .agg(
        F.size(F.collect_set("segment")).alias("num_segments"),
        F.count(F.when(F.col("segment") == "Consumer", 1)).alias("consumer_count"),
        F.count_if(F.col("segment") == "SMB").alias("smb_events"),
        F.countDistinct("segment").alias("distinct_segments"),
    )
    .orderBy(F.col("age_share_tier").desc_nulls_first())
)
