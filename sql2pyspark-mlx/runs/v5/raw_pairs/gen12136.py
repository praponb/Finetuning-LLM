from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())
w_running = Window.partitionBy("city").orderBy("age", "id")

with_last = (
    df_customers
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.started_at") <= F.col("s.signup_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.name",
        "s.email",
        "s.city",
        "s.is_active",
        "s.age",
        F.col("c.started_at").alias("session_started_at"),
    )
)

valid = (
    with_last
    .filter((~F.col("city").eqNullSafe(F.lit("Singapore"))) & (F.to_date("session_started_at") < "2023-05-01"))
    .select(
        "id",
        "name",
        "email",
        "city",
        "is_active",
        "age",
        F.when(F.col("is_active"), "Yes").otherwise("No").alias("is_active_label"),
    )
)

metrics = (
    valid
    .filter(~F.col("is_active"))
    .withColumn("next_age", F.lead("age").over(w_running))
    .withColumn("prev_age", F.lag("age").over(w_running))
    .withColumn("event_seq", F.row_number().over(w_running))
)

cleaned = (
    metrics
    .filter(F.col("city").isNotNull())
    .select("*", F.floor("age").alias("age_whole"))
)

result = (
    cleaned
    .groupBy("city")
    .agg(
        F.count(F.when(F.col("city") == "Singapore", 1)).alias("singapore_rows"),
        F.round(F.avg("age"), 2).alias("avg_age"),
    )
    .orderBy(F.col("singapore_rows").desc_nulls_first())
)
