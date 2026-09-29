from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("country").orderBy("credit_limit", "id")
w3 = Window.partitionBy("segment").orderBy(F.col("age").desc(), "id")
w4 = Window.partitionBy("country").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.end_date").desc(), F.col("c.id").desc())
w6 = Window.partitionBy("first_known_age_tier").orderBy(F.col("avg_first_known_age").desc())

latest_match = (
    df_customers
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.started_at") <= F.col("s.signup_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.country").alias("session_country"))
)

metrics = (
    latest_match
    .filter(F.col("credit_limit") > 12500)
    .withColumn("credit_limit_bucket", F.ntile(10).over(w2))
    .withColumn("age_bucket", F.ntile(10).over(w3))
    .withColumn("first_known_age", F.first("age", ignorenulls=True).over(w4))
)

latest_match_customers = (
    metrics
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.email",
        "s.country",
        "s.city",
        "s.segment",
        "s.signup_date",
        "s.is_active",
        "s.first_known_age",
        F.col("c.end_date"),
        F.col("c.plan"),
    )
)

eligible = (
    latest_match_customers
    .alias("s")
    .join(df_subscriptions.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.start_date") <= F.col("s.end_date")), "left_semi")
)

remaining_keys = (
    eligible
    .select("id", "segment")
    .intersect(
        df_customers
        .filter(F.col("credit_limit") <= 12600)
        .select("id", "segment")
    )
)

kept_rows = (
    eligible
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.segment") == F.col("k.segment")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(
        F.when(F.col("first_known_age") >= 62, "LARGE").otherwise("SMALL").alias("first_known_age_tier"),
        F.date_trunc("YEAR", "end_date").alias("year_start"),
    )
    .agg(
        F.round(F.sum("first_known_age") / F.nullif(F.count(F.when(F.col("plan") == "enterprise", 1)), F.lit(0)), 2).alias("first_known_age_per_enterprise"),
        F.round(F.avg("first_known_age"), 2).alias("avg_first_known_age"),
    )
    .withColumn("avg_first_known_age_dense_rank", F.dense_rank().over(w6))
    .orderBy(F.col("avg_first_known_age").desc())
)
