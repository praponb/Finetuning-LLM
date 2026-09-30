from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("signup_date").desc_nulls_last())
w2 = Window.orderBy("signup_date", "id")
w3 = Window.partitionBy("country")
w4 = Window.partitionBy("segment").orderBy("rank_in_group", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("segment", "country").orderBy("credit_limit", "id")
w6 = Window.partitionBy("segment").orderBy("next_corr_credit_limit_age")
w7 = Window.partitionBy("segment").orderBy("prev_age")
w8 = Window.partitionBy("country", "segment").orderBy("dedup_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_customers
    .filter(F.col("signup_date") >= F.to_date(F.lit("2024-01-10")))
    .select(
        "id",
        "name",
        "email",
        "country",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        F.when(F.col("is_active"), "Yes").otherwise("No").alias("is_active_label"),
        F.coalesce("city", F.lit("OTHER")).alias("city_filled"),
    )
)

deduped = (
    cleaned
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)

scored = (
    deduped
    .withColumn("prev_age", F.lag("age", 2).over(w2))
    .withColumn("corr_credit_limit_age", F.corr("credit_limit", "age").over(w3))
    .withColumn("corr_credit_limit_age_2", F.corr("credit_limit", "age").over(w3))
)

eligible = (
    scored
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.started_at") <= F.col("s.signup_date")), "left_anti")
)

with_windows = (
    eligible
    .select(
        "*",
        F.sum("age").over(w4).alias("running_age"),
        F.lead("corr_credit_limit_age", 1).over(w5).alias("next_corr_credit_limit_age"),
    )
)

latest = (
    with_windows
    .withColumn("dedup_rank", F.rank().over(w6))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    latest
    .withColumn("prev_age_dense_rank", F.dense_rank().over(w7))
    .withColumn("first_known_next_corr_credit_limit_age", F.first("next_corr_credit_limit_age", ignorenulls=True).over(w8))
)
