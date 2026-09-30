from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.monthly_fee").desc(), F.col("c.id").desc())

with_latest = (
    df_customers
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.started_at") <= F.col("s.signup_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.city", F.col("c.device"))
)

latest_match = (
    with_latest
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.monthly_fee"))
)

not_flagged = (
    latest_match
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.customer_id") == 40), "left_anti")
)

filtered = (
    not_flagged
    .filter(F.col("device").isNotNull() & F.col("city").isin(["Tokyo", "Singapore", "New York"]))
    .select("*", F.when(F.col("city") == "Bangkok", 1).otherwise(0).alias("is_bangkok"))
)

result = (
    filtered
    .groupBy(
        F.when(F.col("is_bangkok") >= 69, "HIGH").otherwise("LOW").alias("is_bangkok_tier"),
        "device",
        "city",
    )
    .agg(
        F.round(F.sum("monthly_fee") / F.nullif(F.count(F.when(F.col("city") == "Tokyo", 1)), F.lit(0)), 2).alias("monthly_fee_per_tokyo"),
        F.percentile_approx("is_bangkok", [0.25, 0.5, 0.75]).alias("is_bangkok_quartiles"),
        F.size(F.collect_set("city")).alias("num_cities"),
    )
    .orderBy(F.col("num_cities").asc_nulls_first())
)
