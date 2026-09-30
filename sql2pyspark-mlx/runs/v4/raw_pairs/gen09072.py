from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy(F.col("age").desc())
w2 = Window.partitionBy("segment").orderBy(F.col("age").asc_nulls_first())
w3 = Window.partitionBy("city").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("city")
w5 = Window.orderBy("signup_date", "id").rowsBetween(-6, -1)
w6 = Window.partitionBy("segment").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_customers
    .filter((F.col("signup_date") >= F.lit("2023-06-05").cast("date")) & F.col("signup_date").between("2024-11-01", "2025-08-01"))
    .select(
        "id",
        "name",
        "country",
        "city",
        "segment",
        "signup_date",
        "age",
        F.regexp_replace("name", " ", "_").alias("name_slug"),
        F.lower("email").alias("email_lower"),
        F.coalesce("segment", F.lit("OTHER")).alias("segment_filled"),
    )
)

ranked = (
    filtered
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

not_flagged = (
    ranked
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.order_date") <= F.col("s.signup_date")), "left_semi")
)

enriched = (
    not_flagged
    .select(
        "*",
        F.percent_rank().over(w2).alias("age_percent_rank"),
        F.sum("age").over(w3).alias("cumulative_age"),
        F.max("age").over(w4).alias("city_max_age"),
        F.avg("age").over(w5).alias("age_avg_6_prior"),
        F.sum("age").over(w6).alias("cumulative_age_2"),
    )
)

filtered_customers = (
    enriched
    .filter((F.col("signup_date") >= F.lit("2023-07-12").cast("date")) & F.col("cumulative_age").between(248, 308))
    .select(
        "id",
        "name",
        "country",
        "city",
        "segment",
        "signup_date",
        "age",
        "name_slug",
        "email_lower",
        "age_percent_rank",
        "cumulative_age",
        "city_max_age",
        "cumulative_age_2",
        (F.col("age_percent_rank") - F.col("age")).alias("age_percent_rank_minus_age"),
    )
)

matched_keys = (
    filtered_customers
    .select("id")
    .intersect(
        df_customers
        .filter(F.col("segment").isin(["SMB", "Consumer", "Corporate"]))
        .select("id")
    )
)

reconciled = (
    filtered_customers
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

kept = (
    reconciled
    .alias("s")
    .join(df_accounts.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.opened_date") <= F.col("s.signup_date")), "left_anti")
)

result = (
    kept
    .select(
        "id",
        "name",
        "country",
        "city",
        "segment",
        "signup_date",
        "age",
        "email_lower",
        "age_percent_rank",
    )
    .orderBy(F.col("age_percent_rank").desc_nulls_last(), "signup_date")
)
