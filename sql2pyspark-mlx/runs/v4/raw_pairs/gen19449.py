from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("city")
w2 = Window.partitionBy("city").orderBy(F.col("credit_limit").asc_nulls_first())
w3 = Window.partitionBy("country", "segment")
w4 = Window.partitionBy("segment", "city")

filtered = (
    df_customers
    .filter((F.col("signup_date") > F.to_date(F.lit("2023-09-18"))) & F.col("age").between(40, 49))
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "is_active",
        "credit_limit",
        "age",
        F.coalesce("city", F.lit("N/A")).alias("city_filled"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("city_clean"),
    )
)

remaining_keys = (
    filtered
    .select("id", "country")
    .subtract(
        df_customers
        .filter(F.col("credit_limit").between(9500, 11900))
        .select("id", "country")
    )
)

verified = (
    filtered
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

with_windows = (
    verified
    .select(
        "*",
        F.corr("credit_limit", "age").over(w_hist).alias("corr_credit_limit_age"),
        F.dense_rank().over(w2).alias("credit_limit_dense_rank"),
        F.max("age").over(w_hist).alias("city_max_age"),
        F.round((F.col("age") / F.sum("age").over(w3)), 4).alias("age_share"),
        F.round((F.col("age") / F.sum("age").over(w4)), 4).alias("age_share_2"),
    )
)

base = (
    with_windows
    .filter(F.col("credit_limit").isNotNull())
    .select(
        "*",
        F.floor("age_share_2").alias("age_share_2_whole"),
        F.when(F.col("country").isin(["JP", "TH"]), "Priority").otherwise("Other").alias("country_group"),
        F.coalesce("segment", F.lit("DIRECT")).alias("resolved_segment"),
    )
)

eligible = (
    base
    .alias("s")
    .join(df_sessions.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.customer_id").isNotNull()), "left_anti")
)

kept = (
    eligible
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.device") == "desktop"), "left_anti")
)

result = (
    kept
    .withColumn("age_tier", F.when(F.col("age") >= 63, "LARGE").otherwise("SMALL"))
    .withColumn("corr_credit_limit_age_tier", F.when(F.col("corr_credit_limit_age") >= 0.65, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["age_tier", "corr_credit_limit_age_tier"], ["age_tier"]],
        "age_tier",
        "corr_credit_limit_age_tier",
    )
    .agg(
        F.sort_array(F.collect_set("segment")).alias("segment_list"),
        F.round(F.avg("corr_credit_limit_age"), 2).alias("avg_corr_credit_limit_age"),
        F.round(F.sum("corr_credit_limit_age") / F.nullif(F.count(F.when(F.col("city") == "Tokyo", 1)), F.lit(0)), 2).alias("corr_credit_limit_age_per_tokyo"),
    )
    .filter(F.col("corr_credit_limit_age_per_tokyo") < 0)
    .orderBy(F.col("age_tier").desc_nulls_last())
)
