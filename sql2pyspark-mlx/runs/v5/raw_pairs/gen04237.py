from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("credit_limit").desc(), "id")
w2 = Window.partitionBy("city", "country").orderBy("signup_date")
w3 = Window.partitionBy("city")
w4 = Window.partitionBy("country").orderBy(F.col("rn").desc(), "id")

eligible = (
    df_customers
    .alias("s")
    .join(df_accounts.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.opened_date") >= F.col("s.signup_date")), "left_anti")
)

ranked = (
    eligible
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

not_flagged = (
    ranked
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.status") == "ACTIVE"), "left_anti")
)

latest = (
    not_flagged
    .withColumn("row_num", F.rank().over(w2))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

metrics = (
    latest
    .withColumn("city_stddev_samp_credit_limit", F.stddev_samp("credit_limit").over(w3))
    .withColumn("row_seq", F.row_number().over(w4))
)

result = (
    metrics
    .groupBy(
        F.when(F.col("row_seq") >= 7, "LARGE").otherwise("SMALL").alias("row_seq_tier"),
        F.date_trunc("QUARTER", "signup_date").alias("quarter_start"),
    )
    .agg(
        F.count(F.when(F.col("country") == "DE", 1)).alias("de_rows"),
        F.round(F.sum("credit_limit") / F.nullif(F.count(F.when(F.col("city") == "Chiang Mai", 1)), F.lit(0)), 2).alias("credit_limit_per_chiang_mai"),
    )
)
