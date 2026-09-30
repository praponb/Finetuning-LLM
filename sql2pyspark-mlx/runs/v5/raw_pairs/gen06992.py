from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("signup_date").desc_nulls_first(), "id")
w2 = Window.partitionBy("country").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country").orderBy("latest_rank", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy("country")
w5 = Window.orderBy("credit_limit")
w6 = Window.partitionBy("country").orderBy(F.col("credit_limit_share").desc_nulls_last())
w7 = Window.partitionBy("country").orderBy("credit_limit_avg_6", "id")
w8 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())

valid = (
    df_customers
    .filter(~F.col("city").eqNullSafe(F.lit("New York")))
    .select(
        "id",
        "name",
        "email",
        "country",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        F.when(F.col("is_active"), "Yes").otherwise("No").alias("is_active_label"),
    )
)

latest = (
    valid
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

scored = (
    latest
    .select(
        "*",
        F.first("credit_limit", ignorenulls=True).over(w2).alias("first_known_credit_limit"),
        F.avg("credit_limit").over(w3).alias("credit_limit_avg_6"),
        F.round((F.col("credit_limit") / F.sum("credit_limit").over(w4)), 4).alias("credit_limit_share"),
        F.dense_rank().over(w5).alias("credit_limit_dense_rank"),
    )
)

enriched = (
    scored
    .select(
        "*",
        F.cume_dist().over(w6).alias("credit_limit_share_cume_dist"),
        F.ntile(3).over(w7).alias("credit_limit_avg_6_bucket"),
    )
)

latest_match = (
    enriched
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.started_at") <= F.col("s.signup_date")), "inner")
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.country").alias("session_country"), F.col("c.device"))
)

result = (
    latest_match
    .select(
        "id",
        "email",
        "country",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        "is_active_label",
        "credit_limit_avg_6",
    )
    .orderBy("id")
)
