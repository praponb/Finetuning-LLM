from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy(F.col("monthly_fee").desc(), "id", "subscription_id")
w2 = Window.partitionBy("status").orderBy("opened_date", "id", "subscription_id")

attributed = (
    df_accounts
    .alias("s")
    .join(df_subscriptions.alias("sub"), (F.col("s.customer_id") == F.col("sub.customer_id")) & (F.col("s.opened_date") >= F.col("sub.start_date")) & (F.col("s.opened_date") < F.col("sub.end_date") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "s.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

latest = (
    attributed
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

ranked = (
    latest
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

totals = (
    ranked
    .groupBy(
        F.when(F.col("subscription_id") >= 10, "LARGE").otherwise("SMALL").alias("subscription_id_tier"),
    )
    .agg(
        F.count_if(F.col("plan") == "standard").alias("standard_events"),
        F.percentile_approx("subscription_id", 0.5).alias("subscription_id_p50"),
        F.sort_array(F.collect_set("account_type")).alias("account_type_list"),
    )
)

result = (
    totals
    .select(
        "subscription_id_tier",
        "standard_events",
        "subscription_id_p50",
        "account_type_list",
    )
)
