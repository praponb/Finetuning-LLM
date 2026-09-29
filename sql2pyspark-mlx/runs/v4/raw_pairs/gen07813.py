from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("segment").orderBy("signup_date", "id")
w2 = Window.partitionBy("city", "segment").orderBy("credit_limit", "id")
w3 = Window.partitionBy("country").orderBy(F.col("credit_limit").desc(), "id")
w_grp = Window.partitionBy("segment").orderBy(F.col("age").asc_nulls_first())
w5 = Window.partitionBy("country")
w6 = Window.partitionBy("segment").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("segment").orderBy(F.col("next_credit_limit").asc_nulls_last())
w8 = Window.partitionBy("city")
w9 = Window.orderBy("credit_limit_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("city", "country").orderBy("credit_limit_bucket", "id")
w11 = Window.partitionBy("country").orderBy(F.col("row_seq").desc(), "id")
w12 = Window.partitionBy("country", "city").orderBy(F.col("age_cume_dist").desc_nulls_last(), "id")
w13 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.opened_date").desc(), F.col("c.id").desc())
w14 = Window.partitionBy("country").orderBy("running_credit_limit")
w15 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.monthly_fee").desc(), F.col("c.id").desc())

with_windows = (
    df_customers
    .withColumn("next_credit_limit", F.lead("credit_limit", 2).over(w_running))
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("credit_limit_bucket", F.ntile(5).over(w3))
)

metrics = (
    with_windows
    .withColumn("age_cume_dist", F.cume_dist().over(w_grp))
    .withColumn("country_stddev_samp_next_credit_limit", F.stddev_samp("next_credit_limit").over(w5))
    .withColumn("cumulative_next_credit_limit", F.sum("next_credit_limit").over(w6))
    .withColumn("next_credit_limit_dense_rank", F.dense_rank().over(w_ordered))
    .withColumn("city_max_credit_limit", F.max("credit_limit").over(w8))
)

enriched = (
    metrics
    .select(
        "*",
        F.sum("credit_limit").over(w9).alias("running_credit_limit"),
        F.stddev_samp("cumulative_next_credit_limit").over(w5).alias("country_stddev_samp_cumulative_next_credit_limit"),
        F.ntile(10).over(w10).alias("credit_limit_bucket_bucket"),
        F.ntile(3).over(w11).alias("row_seq_bucket"),
        F.row_number().over(w2).alias("row_seq_2"),
    )
)

ranked = (
    enriched
    .withColumn("row_num", F.row_number().over(w12))
    .filter(F.col("row_num") == 1)
)

latest_match = (
    ranked
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.opened_date") <= F.col("s.signup_date")), "left")
    .withColumn("rn", F.row_number().over(w13))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.balance").alias("account_balance"))
)

ranked_customers = (
    latest_match
    .withColumn("rn", F.rank().over(w14))
    .filter(F.col("rn") == 1)
)

with_latest = (
    ranked_customers
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn_2", F.row_number().over(w15))
    .filter(F.col("rn_2") == 1)
    .select("s.*", F.col("c.start_date"))
)

result = (
    with_latest
    .select(
        "id",
        "name",
        "email",
        "country",
        "segment",
        "signup_date",
        "is_active",
        "age",
        "credit_limit_bucket",
        F.when(F.col("segment").rlike("^(Consumer|SMB)$"), "PRIORITY").when(F.col("segment").rlike("^(Corporate)$"), "GROUP_B").otherwise("EXTENDED").alias("segment_zone"),
    )
)
