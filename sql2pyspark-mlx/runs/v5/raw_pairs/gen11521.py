from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("segment").orderBy(F.col("credit_limit").desc_nulls_last())
w3 = Window.partitionBy("city").orderBy("started_at", "id")
w4 = Window.partitionBy("city").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_customers
    .filter((F.col("signup_date") >= F.lit("2023-07-15").cast("date")) & ~F.col("is_active"))
    .select(
        "id",
        "email",
        "country",
        "city",
        "segment",
        "is_active",
        "credit_limit",
        "age",
        F.month("signup_date").alias("signup_date_month"),
        F.concat("segment", F.lit(" "), "city").alias("segment_city"),
    )
)

latest_match = (
    base
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.started_at"), F.col("c.device"))
)

deduped = (
    latest_match
    .withColumn("latest_rank", F.rank().over(w2))
    .filter(F.col("latest_rank") == 1)
)

with_gaps = (
    deduped
    .withColumn("new_group_flag", F.when(F.lag("started_at").over(w3).isNull() | (F.col("started_at") > F.lag("started_at").over(w3) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_no", F.sum("new_group_flag").over(w4))

not_flagged = (
    sessionized
    .alias("s")
    .join(df_sessions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.started_at") >= F.col("s.started_at")), "left_anti")
)

eligible = (
    not_flagged
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.opened_date").between("2024-11-01", "2025-02-01")), "left_anti")
)

result = (
    eligible
    .select(
        "id",
        "email",
        "country",
        "segment",
        "age",
        "signup_date_month",
        "segment_city",
        "started_at",
        "latest_rank",
    )
)
