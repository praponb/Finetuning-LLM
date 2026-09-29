from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.monthly_fee").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("city", "segment").orderBy(F.col("credit_limit").desc_nulls_first(), "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("country").orderBy(F.col("signup_date").desc(), "id")
w5 = Window.partitionBy("segment", "city").orderBy("latest_rank", "id")
w6 = Window.partitionBy("session_device").orderBy(F.col("age").desc(), "id")
w_recent = Window.partitionBy("country").orderBy("latest_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("segment").orderBy("latest_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("country").orderBy(F.col("seq_no").desc_nulls_first(), "id")

with_latest = (
    df_customers
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.monthly_fee").alias("subscription_monthly_fee"))
)

latest = (
    with_latest
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

with_last = (
    latest
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.email",
        "s.country",
        "s.city",
        "s.segment",
        "s.signup_date",
        "s.age",
        "s.latest_rank",
        F.col("c.device").alias("session_device"),
        F.col("c.started_at"),
    )
)

enriched = (
    with_last
    .select(
        "*",
        F.row_number().over(w4).alias("seq_no"),
        F.row_number().over(w5).alias("row_seq"),
        F.ntile(3).over(w6).alias("age_bucket"),
        F.last("session_device", ignorenulls=True).over(w_recent).alias("last_known_session_device"),
        F.sum("age").over(w8).alias("running_age"),
    )
)

deduped = (
    enriched
    .withColumn("rn", F.row_number().over(w9))
    .filter(F.col("rn") == 1)
)

result = (
    deduped
    .select(
        "id",
        "email",
        "country",
        "city",
        "signup_date",
        "age",
        "latest_rank",
        "started_at",
        "seq_no",
        F.when(F.col("city").rlike("^(Berlin|Chiang Mai)$"), "GROUP_B").when(F.col("city").rlike("^(Bangkok|New York)$"), "EXTENDED").otherwise("OTHER_ZONE").alias("city_zone"),
    )
    .orderBy(F.col("signup_date").desc())
)
