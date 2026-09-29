from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("subscription_plan").orderBy("start_date", "id", "subscription_id")
w2 = Window.orderBy("credit_limit", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy("subscription_id", "id", "subscription_id").rowsBetween(-5, Window.currentRow)

base = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull() & (F.col("end_date") >= F.lit("2023-06-17").cast("date")))
    .select("*", F.initcap(F.trim("status")).alias("status_title"))
)

matched = (
    base
    .alias("e")
    .join(df_subscriptions.alias("sub"), (F.col("e.customer_id") == F.col("sub.customer_id")) & (F.col("e.start_date") >= F.col("sub.start_date")) & (F.col("e.start_date") < F.col("sub.end_date") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "e.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

latest = (
    matched
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

joined = (
    latest
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "left")
    .select(
        "e.id",
        "e.customer_id",
        "e.start_date",
        "e.end_date",
        "e.status",
        "e.subscription_id",
        "e.subscription_plan",
        "e.subscription_monthly_fee",
        F.col("p.email"),
        F.col("p.credit_limit"),
        F.col("p.name"),
    )
)

scored = (
    joined
    .filter(F.col("subscription_monthly_fee").between(90, 210))
    .select(
        "id",
        "customer_id",
        "end_date",
        "subscription_id",
        "credit_limit",
        "name",
        F.sum("subscription_monthly_fee").over(w2).alias("cumulative_subscription_monthly_fee"),
        F.avg("subscription_id").over(w3).alias("subscription_id_avg_5"),
    )
)

result = (
    scored
    .groupBy("subscription_id")
    .agg(
        F.max("credit_limit").alias("max_credit_limit"),
        F.percentile_approx("credit_limit", [0.25, 0.5, 0.75]).alias("credit_limit_quartiles"),
        F.count("*").alias("row_count"),
        F.count("*").alias("num_rows"),
    )
    .filter(F.col("max_credit_limit") > 0)
    .orderBy(F.col("subscription_id").asc_nulls_last())
)
