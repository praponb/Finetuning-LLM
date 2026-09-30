from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key", "customer_id").orderBy("created_at", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id", "attr_key").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)
w3 = Window.partitionBy("customer_id")
w4 = Window.partitionBy("customer_id").orderBy("created_at", "id", "attr_key")
w5 = Window.partitionBy("customer_id").orderBy(F.col("customer_id_sum_age").desc())

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("attr_key", "value"),
    )
)

scored = (
    flattened
    .withColumn("last_known_attr_key", F.last("attr_key", ignorenulls=True).over(w))
)

joined = (
    scored
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "inner")
    .select("src.*", F.col("c.age"), F.col("c.signup_date"))
)

enriched = (
    joined
    .select(
        "id",
        "customer_id",
        "attr_key",
        "value",
        "signup_date",
        F.avg("age").over(w2).alias("age_avg_15m"),
        F.sum("age").over(w3).alias("customer_id_sum_age"),
        F.lag("age", 1).over(w4).alias("prev_age"),
    )
)

latest = (
    enriched
    .withColumn("latest_rank", F.rank().over(w5))
    .filter(F.col("latest_rank") == 1)
)

result = (
    latest
    .rollup(
        F.when(F.col("age_avg_15m") >= 74, "HIGH").otherwise("LOW").alias("age_avg_15m_tier"),
        "customer_id",
    )
    .agg(
        F.sum(F.when(F.col("attr_key") == "gift", F.col("customer_id_sum_age")).otherwise(0)).alias("gift_customer_id_sum_age"),
        F.percentile_approx("customer_id_sum_age", [0.25, 0.5, 0.75]).alias("customer_id_sum_age_quartiles"),
        F.sum(F.when(F.col("attr_key") == "device", F.col("customer_id_sum_age")).otherwise(0)).alias("device_customer_id_sum_age"),
    )
    .orderBy(F.col("age_avg_15m_tier").desc_nulls_last())
)
