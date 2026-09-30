from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("opened_date").asc_nulls_last(), "id")
w2 = Window.partitionBy("customer_id").orderBy(F.col("subscription_id").desc(), "id", "subscription_id")
w3 = Window.partitionBy("plan").orderBy(F.col("subscription_id").desc(), "id", "subscription_id")
w4 = Window.partitionBy("customer_id").orderBy("rn", "id", "subscription_id").rowsBetween(-2, -1)
w5 = Window.partitionBy("account_type").orderBy(F.col("monthly_fee").asc_nulls_first(), "id", "subscription_id")
w6 = Window.partitionBy("status", "customer_id").orderBy(F.col("rn").desc())

deduped = (
    df_accounts
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

attributed = (
    deduped
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & F.col("src.opened_date").between(F.col("sub.start_date"), F.col("sub.end_date")), "left")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

metrics = (
    attributed
    .withColumn("subscription_id_bucket", F.ntile(3).over(w2))
    .withColumn("row_seq", F.row_number().over(w3))
    .withColumn("balance_avg_2_prior", F.avg("balance").over(w4))
)

latest = (
    metrics
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
)

latest_accounts = (
    latest
    .withColumn("row_num", F.rank().over(w6))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest_accounts
    .groupBy("subscription_id")
    .agg(
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.round(F.sum("monthly_fee") / F.nullif(F.count(F.when(F.col("plan") == "enterprise", 1)), F.lit(0)), 2).alias("monthly_fee_per_enterprise"),
        F.avg("subscription_id").alias("avg_subscription_id"),
    )
)
