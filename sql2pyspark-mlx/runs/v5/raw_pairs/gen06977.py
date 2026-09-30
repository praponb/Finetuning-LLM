from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("age_times_credit_limit").asc_nulls_last(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("session_country").orderBy(F.col("session_started_at").asc_nulls_first(), "id")

cleaned = (
    df_customers
    .filter(F.col("signup_date") >= F.lit("2023-08-20").cast("date"))
    .select(
        "id",
        "name",
        "country",
        "city",
        "segment",
        "is_active",
        "credit_limit",
        (F.col("age") * F.col("credit_limit")).alias("age_times_credit_limit"),
        F.coalesce("segment", F.lit("NONE")).alias("segment_clean"),
    )
)

ranked = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "country", "city", "credit_limit", "segment_clean")
)

with_last = (
    ranked
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.country",
        "s.credit_limit",
        "s.segment_clean",
        F.col("c.started_at").alias("session_started_at"),
        F.col("c.country").alias("session_country"),
    )
)

latest = (
    with_last
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    latest
    .select(
        "id",
        "country",
        "credit_limit",
        "segment_clean",
        "session_country",
        "dedup_rank",
        F.when(F.col("session_country").rlike("^(JP|SG)$"), "GROUP_B").when(F.col("session_country").rlike("^(TH|US)$"), "PRIORITY").otherwise("EXTENDED").alias("session_country_zone"),
    )
)
