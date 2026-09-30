from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at")

latest = (
    df_sessions
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "customer_id", "device", "browser", "started_at", "country")
)

cleaned = (
    latest
    .filter(F.col("customer_id").isNotNull() & F.col("customer_id").isNotNull())
    .withColumn("browser_length", F.length("browser"))
    .withColumn("browser_filled", F.coalesce("browser", F.lit("OTHER")))
)

joined = (
    cleaned
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id").eqNullSafe(F.col("d.id")), "left")
    .select("s.*", F.col("d.credit_limit"), F.col("d.email"), F.col("d.city"))
)

matched_keys = (
    joined
    .select("country", "customer_id")
    .intersect(
        df_sessions
        .filter(F.col("device").isin(["tablet", "desktop", "mobile"]))
        .select("country", "customer_id")
    )
)

verified = (
    joined
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.country") == F.col("k.country")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

summary = (
    verified
    .groupBy(
        F.when(F.col("credit_limit") >= 12400, "LARGE").otherwise("SMALL").alias("credit_limit_tier"),
    )
    .agg(
        F.min("started_at").alias("started_at"),
        F.sum(F.when(F.col("country") == "US", F.col("browser_length")).otherwise(0)).alias("us_browser_length"),
        F.min("credit_limit").alias("min_credit_limit"),
        F.sort_array(F.collect_set("browser")).alias("browser_list"),
        F.max("credit_limit").alias("max_credit_limit"),
        F.countDistinct("country").alias("distinct_countries"),
    )
)

result = (
    summary
    .select(
        "credit_limit_tier",
        "started_at",
        "us_browser_length",
        "min_credit_limit",
        "browser_list",
        "max_credit_limit",
    )
    .orderBy(F.col("max_credit_limit").desc(), "started_at")
)
