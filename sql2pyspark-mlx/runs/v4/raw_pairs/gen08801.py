from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("age", "id")
w_recent = Window.partitionBy("city")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())
w4 = Window.orderBy(F.col("age").desc(), "id")
w5 = Window.orderBy("signup_date", "id")
w6 = Window.partitionBy("segment").orderBy("age", "id")

cleaned = (
    df_customers
    .filter(F.col("signup_date") > F.lit("2024-02-25").cast("date"))
    .select(
        "id",
        "name",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "age",
        F.round("credit_limit", 2).alias("credit_limit_rounded"),
        F.coalesce("city", F.lit("DIRECT")).alias("resolved_city"),
    )
)

with_windows = (
    cleaned
    .select(
        "*",
        F.lag("credit_limit_rounded", 1).over(w).alias("prev_credit_limit_rounded"),
        F.min("age").over(w_recent).alias("city_min_age"),
    )
)

latest_match = (
    with_windows
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.started_at") <= F.col("s.signup_date")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.started_at"))
)

result = (
    latest_match
    .select(
        "*",
        F.row_number().over(w4).alias("row_seq"),
        F.row_number().over(w5).alias("seq_no"),
        F.ntile(5).over(w6).alias("age_bucket"),
    )
)
