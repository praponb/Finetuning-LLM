from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("credit_limit").desc(), "id")

filtered = (
    df_customers
    .filter((F.col("signup_date") > F.lit("2023-08-03").cast("date")) & (F.col("credit_limit") < 11300))
    .select(
        "*",
        F.concat("segment", F.lit("/"), "country").alias("segment_country"),
        F.initcap(F.trim("city")).alias("city_title"),
    )
)

deduped = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

kept_keys = (
    deduped
    .select("id")
    .subtract(
        df_customers
        .filter(F.col("credit_limit").between(7100, 12100))
        .select("id")
    )
)

verified = (
    deduped
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

daily = (
    verified
    .groupBy(
        F.date_trunc("WEEK", "signup_date").alias("week_start"),
        F.when(F.col("credit_limit") >= 12600, "PREMIUM").otherwise("STANDARD").alias("credit_limit_tier"),
        "city",
    )
    .agg(
        F.sort_array(F.collect_set("segment")).alias("segment_list"),
        F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"),
        F.count("*").alias("event_count"),
        F.round(F.sum("credit_limit") / F.nullif(F.count(F.when(F.col("segment") == "Consumer", 1)), F.lit(0)), 2).alias("credit_limit_per_consumer"),
        F.sum(F.when(F.col("city") == "Bangkok", F.col("age")).otherwise(0)).alias("bangkok_age"),
        F.bit_or("age").alias("age_bit_or"),
    )
    .filter(F.col("credit_limit_per_consumer") < 0)
)

result = (
    daily
    .select(
        "week_start",
        "credit_limit_tier",
        "city",
        "segment_list",
        "avg_credit_limit",
        "event_count",
        "credit_limit_per_consumer",
        "bangkok_age",
        "age_bit_or",
    )
)
