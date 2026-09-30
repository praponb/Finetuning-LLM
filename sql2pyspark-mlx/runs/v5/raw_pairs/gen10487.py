from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("segment", "city").orderBy("age", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("country", "segment")
w3 = Window.partitionBy("segment").orderBy("segment_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.opened_date").desc(), F.col("c.id").desc())

cleaned = (
    df_customers
    .filter(F.col("city").isNotNull() & (F.col("city").eqNullSafe(F.lit("New York"))))
    .withColumn("segment_length", F.length("segment"))
)

scored = (
    cleaned
    .select(
        "*",
        F.max("age").over(w_grp).alias("last7_max_age"),
        F.sum("age").over(w_grp).alias("last7_sum_age"),
        F.avg("age").over(w_grp).alias("last7_avg_age"),
    )
)

with_windows = (
    scored
    .select(
        "*",
        F.round((F.col("last7_avg_age") / F.sum("last7_avg_age").over(w2)), 4).alias("last7_avg_age_share"),
        F.sum("last7_avg_age").over(w3).alias("cumulative_last7_avg_age"),
    )
)

not_flagged = (
    with_windows
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.start_date") >= F.col("s.signup_date")), "left_semi")
)

with_last = (
    not_flagged
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.status").alias("account_status"),
        F.col("c.account_type").alias("account_account_type"),
    )
)

result = (
    with_last
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "credit_limit",
        "segment_length",
    )
    .orderBy("segment")
)
