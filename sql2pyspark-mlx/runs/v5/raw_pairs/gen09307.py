from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment")
w2 = Window.partitionBy("country").orderBy("credit_limit_int", "id").rowsBetween(-2, -1)
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.end_date").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("city").orderBy(F.col("age").desc_nulls_last(), "id")
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.balance").desc(), F.col("c.id").desc())

base = (
    df_customers
    .filter(F.col("city").isNotNull())
    .withColumn("credit_limit_int", F.col("credit_limit").cast("int"))
    .withColumn("is_active_label", F.when(F.col("is_active"), "Yes").otherwise("No"))
)

metrics = (
    base
    .filter(F.col("credit_limit") >= 7500)
    .select(
        "*",
        F.corr("age", "credit_limit").over(w).alias("corr_age_credit_limit"),
        F.avg("credit_limit").over(w2).alias("credit_limit_avg_2_prior"),
    )
)

with_latest = (
    metrics
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.name",
        "s.city",
        "s.segment",
        "s.is_active",
        "s.credit_limit",
        "s.age",
        "s.credit_limit_int",
        "s.is_active_label",
        "s.corr_age_credit_limit",
        "s.credit_limit_avg_2_prior",
        F.col("c.monthly_fee"),
    )
)

deduped = (
    with_latest
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

latest_match = (
    deduped
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.status"), F.col("c.balance").alias("account_balance"))
)

totals = (
    latest_match
    .groupBy("status")
    .agg(
        F.count_if(F.col("city") == "Chiang Mai").alias("chiang_mai_events"),
        F.stddev_samp("age").alias("age_stddev"),
        F.bit_or("age").alias("age_bit_or"),
        F.count_if(F.col("segment") == "Corporate").alias("corporate_events"),
        F.count_if(F.col("segment") == "Consumer").alias("consumer_events"),
    )
)

result = (
    totals
    .select(
        "status",
        "chiang_mai_events",
        "corporate_events",
        "consumer_events",
        F.when(F.col("status").rlike("^(FROZEN|CLOSED)$"), "CORE").otherwise("OTHER_ZONE").alias("status_zone"),
    )
    .orderBy(F.col("status").desc())
)
