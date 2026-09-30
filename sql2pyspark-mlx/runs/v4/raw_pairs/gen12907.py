from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("created_at", "id", "attr_key")
w2 = Window.partitionBy("country").orderBy("created_at", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

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

joined = (
    flattened
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.signup_date"))
)

joined_carts = (
    joined
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.credit_limit"), F.col("d.country"))
)

flagged = (
    joined_carts
    .withColumn("session_start_flag", F.when(F.col("created_at") - F.lag("created_at").over(w) > F.expr("INTERVAL 15 MINUTES"), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("session_start_flag").over(w2))

result = (
    sessionized
    .groupBy(
        F.when(F.col("session_start_flag") >= 1, "PREMIUM").otherwise("STANDARD").alias("session_start_flag_tier"),
        F.date_trunc("HOUR", "created_at").alias("hour_window"),
    )
    .agg(
        F.countDistinct("attr_key").alias("distinct_attr_keys"),
        F.percentile_approx("burst_id", 0.5).alias("burst_id_p50"),
        F.count(F.when(F.col("country") == "JP", 1)).alias("jp_count"),
        F.percentile_approx("session_start_flag", [0.25, 0.5, 0.75]).alias("session_start_flag_quartiles"),
    )
    .orderBy(F.col("hour_window").asc_nulls_first())
)
