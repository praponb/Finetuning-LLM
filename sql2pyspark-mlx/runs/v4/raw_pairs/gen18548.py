from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("age").asc_nulls_first())
w2 = Window.orderBy("age")

filtered = (
    df_customers
    .filter(F.col("city").isNotNull() & F.upper("email").like("%IAM%"))
    .select(
        "*",
        F.when(F.col("city").isin(["London", "Tokyo"]), "Priority").otherwise("Other").alias("city_group"),
    )
)

eligible = (
    filtered
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.store_id").isNotNull()), "left_anti")
)

cleaned = (
    eligible
    .filter(F.col("city").isin(["Bangkok", "Tokyo"]))
    .select(
        "id",
        "country",
        "city",
        "segment",
        "signup_date",
        "credit_limit",
        "age",
        "city_group",
        F.lower("city").alias("city_lower"),
        F.last_day("signup_date").alias("signup_date_month_end"),
        F.coalesce("segment", F.lit("N/A")).alias("resolved_segment"),
    )
)

matched_keys = (
    cleaned
    .select("segment")
    .exceptAll(
        df_customers
        .filter(F.year("signup_date") >= 2024)
        .select("segment")
    )
)

reconciled = (
    cleaned
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.segment") == F.col("k.segment")), "inner")
    .select("s.*")
)

remaining_keys = (
    reconciled
    .select("segment")
    .intersectAll(
        df_customers
        .filter(F.col("credit_limit").isNotNull())
        .select("segment")
    )
)

reconciled_customers = (
    reconciled
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.segment") == F.col("k.segment")), "inner")
    .select("s.*")
)

scored = (
    reconciled_customers
    .withColumn("age_dense_rank", F.dense_rank().over(w))
    .withColumn("age_dense_rank_2", F.dense_rank().over(w2))
)

stats = (
    scored
    .cube(
        F.when(F.col("age_dense_rank_2") >= 7, "HIGH").otherwise("LOW").alias("age_dense_rank_2_tier"),
        "city",
    )
    .agg(
        F.round(F.avg("age"), 2).alias("avg_age"),
        F.count_if(F.col("country") == "US").alias("us_events"),
        F.size(F.collect_set("city")).alias("num_cities"),
        F.count(F.when(F.col("city") == "Berlin", 1)).alias("berlin_rows"),
        F.round(F.sum("age") / F.nullif(F.count(F.when(F.col("city") == "Singapore", 1)), F.lit(0)), 2).alias("age_per_singapore"),
        F.sum(F.when(F.col("country") == "SG", F.col("age_dense_rank")).otherwise(0)).alias("sg_age_dense_rank"),
    )
    .filter(F.col("us_events") > 1)
)

result = (
    stats
    .select(
        "age_dense_rank_2_tier",
        "city",
        "avg_age",
        "num_cities",
        "berlin_rows",
        "sg_age_dense_rank",
    )
)
