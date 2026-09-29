from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("age").desc(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.balance").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("country", "city").orderBy("signup_date", "id").rowsBetween(-5, -1)
w_ordered = Window.partitionBy("segment")
w_recent = Window.orderBy(F.col("credit_limit").asc_nulls_last())
w6 = Window.orderBy(F.col("credit_limit").desc(), "id")
w7 = Window.partitionBy("country").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("account_type", "city").orderBy("credit_limit_avg_5_prior", "id")

latest = (
    df_customers
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_last = (
    latest
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.account_type"), F.col("c.opened_date"))
)

with_windows = (
    with_last
    .withColumn("credit_limit_avg_5_prior", F.avg("credit_limit").over(w3))
    .withColumn("corr_age_credit_limit", F.corr("age", "credit_limit").over(w_ordered))
    .withColumn("credit_limit_dense_rank", F.dense_rank().over(w_recent))
)

scored = (
    with_windows
    .withColumn("event_seq", F.row_number().over(w6))
    .withColumn("running_corr_age_credit_limit", F.sum("corr_age_credit_limit").over(w7))
    .withColumn("row_seq", F.row_number().over(w8))
)

result = (
    scored
    .groupBy(
        F.when(F.col("credit_limit_avg_5_prior") >= 15433, "PREMIUM").otherwise("STANDARD").alias("credit_limit_avg_5_prior_tier"),
    )
    .agg(
        F.max("credit_limit_avg_5_prior").alias("max_credit_limit_avg_5_prior"),
        F.percentile_approx("row_seq", [0.25, 0.5, 0.75]).alias("row_seq_quartiles"),
        F.count_if(F.col("city") == "London").alias("london_events"),
        F.bit_or("age").alias("age_bit_or"),
    )
    .filter(F.col("max_credit_limit_avg_5_prior") >= 0)
    .orderBy(F.col("credit_limit_avg_5_prior_tier").desc_nulls_first())
)
