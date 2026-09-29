from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("customer_id")
w_hist = Window.partitionBy("attr_key").orderBy(F.col("age").desc_nulls_last())
w3 = Window.orderBy(F.col("age").asc_nulls_last())

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("attr_key", "v"),
    )
)

valid = (
    lines
    .filter((F.col("created_at") >= F.lit("2025-03-01").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "attr_key",
        "v",
        F.lower("attr_key").alias("attr_key_lower"),
        F.lower("attr_key").alias("attr_key_lower_2"),
    )
)

enriched = (
    valid
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "left")
    .select("src.*", F.col("c.age"))
)

result = (
    enriched
    .withColumn("customer_id_avg_age", F.avg("age").over(w_running))
    .withColumn("customer_id_count_age", F.count("age").over(w_running))
    .withColumn("age_cume_dist", F.cume_dist().over(w_hist))
    .withColumn("age_percent_rank", F.percent_rank().over(w3))
)
