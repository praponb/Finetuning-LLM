from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("country", "segment").orderBy("is_uk", "id")
w2 = Window.partitionBy("next_credit_limit_tier").orderBy(F.col("uk_avg_next_age_adjusted").desc())

base = (
    df_customers
    .filter((F.col("signup_date") >= F.lit("2023-06-20").cast("date")) & F.col("segment").isNotNull())
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
        F.when(F.col("country") == "UK", 1).otherwise(0).alias("is_uk"),
        F.least("age", "credit_limit").alias("least_age_credit_limit"),
        F.coalesce("city", F.lit("N/A")).alias("resolved_city"),
    )
)

with_windows = (
    base
    .filter(F.col("segment") != "Corporate")
    .select(
        "*",
        F.lead("age").over(w_running).alias("next_age"),
        F.lead("credit_limit").over(w_running).alias("next_credit_limit"),
        F.row_number().over(w_running).alias("row_seq"),
    )
)

eligible = (
    with_windows
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.customer_id").isNotNull()), "left_semi")
)

kept = (
    eligible
    .alias("s")
    .join(df_accounts.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.opened_date") >= "2025-03-01"), "left_anti")
)

kept_customers = (
    kept
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.channel") != "partner"), "left_anti")
)

cleaned = (
    kept_customers
    .filter(F.col("segment").isNotNull() & (F.col("age") < 49))
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "least_age_credit_limit",
        "next_age",
        "next_credit_limit",
        "row_seq",
        F.when(F.col("city") == "London", 1).otherwise(0).alias("is_london"),
        F.round((F.col("next_age") * 0.9), 2).alias("next_age_adjusted"),
    )
)

stats = (
    cleaned
    .groupBy(
        F.when(F.col("next_credit_limit") >= 15900, "HIGH").otherwise("LOW").alias("next_credit_limit_tier"),
        F.when(F.col("next_age_adjusted") >= 77, "HIGH").otherwise("LOW").alias("next_age_adjusted_tier"),
    )
    .agg(
        F.avg(F.when(F.col("country") == "UK", F.col("next_age_adjusted"))).alias("uk_avg_next_age_adjusted"),
        F.count(F.when(F.col("country") == "DE", 1)).alias("de_count"),
        F.round(F.avg("row_seq"), 2).alias("avg_row_seq"),
        F.count(F.when(F.col("segment") == "SMB", 1)).alias("smb_rows"),
        F.countDistinct("segment").alias("distinct_segments"),
    )
    .filter(F.col("avg_row_seq") > 0)
    .withColumn("uk_avg_next_age_adjusted_rank", F.rank().over(w2))
)

result = (
    stats
    .select(
        "next_credit_limit_tier",
        "next_age_adjusted_tier",
        "uk_avg_next_age_adjusted",
        "de_count",
        "avg_row_seq",
        "smb_rows",
        "distinct_segments",
        "uk_avg_next_age_adjusted_rank",
    )
    .orderBy(F.col("distinct_segments").desc())
)
