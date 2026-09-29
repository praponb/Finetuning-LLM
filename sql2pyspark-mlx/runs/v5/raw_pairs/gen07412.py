from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())
w_running = Window.partitionBy("session_browser").orderBy("age", "id").rowsBetween(-6, -1)
w3 = Window.partitionBy("session_browser").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("session_browser").orderBy("age", "id")

valid = (
    df_customers
    .filter(~F.col("email").like("%@mail.co.th"))
    .select(
        "id",
        "email",
        "country",
        "segment",
        "is_active",
        "age",
        F.col("credit_limit").cast("int").alias("credit_limit_int"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("city_clean"),
    )
)

with_last = (
    valid
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.email",
        "s.is_active",
        "s.age",
        "s.city_clean",
        F.col("c.browser").alias("session_browser"),
    )
)

not_flagged = (
    with_last
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.status").isin(["COMPLETED", "RETURNED", "SHIPPED"])), "left_semi")
)

scored = (
    not_flagged
    .filter(F.col("session_browser") == "Chrome")
    .select(
        "id",
        "email",
        "is_active",
        "city_clean",
        F.avg("age").over(w_running).alias("age_avg_6_prior"),
        F.sum("age").over(w3).alias("running_age"),
        F.ntile(4).over(w4).alias("age_bucket"),
    )
)

not_flagged_customers = (
    scored
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.customer_id").isNotNull()), "left_semi")
)

remaining_keys = (
    not_flagged_customers
    .select("id")
    .intersect(
        df_customers
        .filter(F.col("signup_date") >= "2023-10-01")
        .select("id")
    )
)

verified = (
    not_flagged_customers
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select("id", "email", "city_clean", "age_avg_6_prior", "running_age")
    .orderBy(F.col("running_age").desc())
)
