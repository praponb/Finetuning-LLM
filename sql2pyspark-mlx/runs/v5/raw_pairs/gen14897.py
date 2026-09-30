from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "status").orderBy(F.col("credit_limit").desc(), "id")
w2 = Window.partitionBy("status").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("plan").orderBy("running_min_credit_limit", "id").rowsBetween(-6, Window.currentRow)

enriched = (
    df_subscriptions
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "inner")
    .filter(F.col("d.country") == "TH")
    .select("s.*", F.col("d.signup_date"), F.col("d.email"), F.col("d.credit_limit"))
)

ranked = (
    enriched
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

scored = (
    ranked
    .select(
        "id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "signup_date",
        "email",
        "credit_limit",
        F.min("credit_limit").over(w2).alias("running_min_credit_limit"),
        F.max("credit_limit").over(w2).alias("running_max_credit_limit"),
        F.count("credit_limit").over(w2).alias("running_count_credit_limit"),
        F.avg("credit_limit").over(w2).alias("running_avg_credit_limit"),
        F.last("credit_limit", ignorenulls=True).over(w3).alias("last_known_credit_limit"),
    )
)

enriched_subscriptions = (
    scored
    .select(
        "id",
        "signup_date",
        "credit_limit",
        "running_max_credit_limit",
        "running_avg_credit_limit",
        "last_known_credit_limit",
        F.count("last_known_credit_limit").over(w_recent).alias("last7_count_last_known_credit_limit"),
        F.min("last_known_credit_limit").over(w_recent).alias("last7_min_last_known_credit_limit"),
        F.avg("last_known_credit_limit").over(w_recent).alias("last7_avg_last_known_credit_limit"),
    )
)

result = (
    enriched_subscriptions
    .select(
        "id",
        "signup_date",
        "running_max_credit_limit",
        "running_avg_credit_limit",
        "last_known_credit_limit",
        "last7_min_last_known_credit_limit",
        "last7_avg_last_known_credit_limit",
    )
    .orderBy(
        F.col("last7_min_last_known_credit_limit").asc_nulls_last(),
        F.col("last7_avg_last_known_credit_limit").desc(),
    )
)
