from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("session_browser")
w3 = Window.partitionBy("segment")

with_latest = (
    df_customers
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.browser").alias("session_browser"))
)

filtered = (
    with_latest
    .filter(F.col("session_browser").isNotNull() & (F.col("signup_date") >= F.lit("2023-02-28").cast("date")))
    .select(
        "*",
        F.trim("email").alias("clean_email"),
        F.coalesce("city", F.lit("N/A")).alias("resolved_city"),
    )
)

not_flagged = (
    filtered
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.store_id") == 33), "left_anti")
)

metrics = (
    not_flagged
    .withColumn("session_browser_max_age", F.max("age").over(w2))
    .withColumn("age_share", F.round((F.col("age") / F.sum("age").over(w3)), 4))
)

valid = (
    metrics
    .filter((F.col("city") == "Chiang Mai") & F.col("segment").isNotNull())
    .withColumn("is_bangkok", F.when(F.col("city") == "Bangkok", 1).otherwise(0))
    .withColumn("session_browser_filled", F.coalesce("session_browser", F.lit("NONE")))
)

base = (
    valid
    .filter((F.col("signup_date") >= F.lit("2023-03-30").cast("date")) & (F.col("is_bangkok") < 46))
    .withColumn("clean_email_2", F.trim("email"))
    .withColumn("city_group", F.when(F.col("city").isin(["Bangkok", "London"]), "Group A").otherwise("Other"))
)

result = (
    base
    .groupBy(
        F.when(F.col("is_bangkok") >= 70, "PREMIUM").otherwise("STANDARD").alias("is_bangkok_tier"),
        F.when(F.col("age") >= 61, "HIGH").otherwise("LOW").alias("age_tier"),
    )
    .agg(
        F.countDistinct("city").alias("distinct_cities"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("session_browser_max_age"), 2).alias("avg_session_browser_max_age"),
        F.sort_array(F.collect_set("session_browser")).alias("session_browser_list"),
        F.sort_array(F.collect_set("session_browser")).alias("session_browser_list_2"),
        F.size(F.collect_set("segment")).alias("num_segments"),
    )
    .filter(F.col("num_segments") >= 1)
    .orderBy(F.col("num_segments").desc())
)
