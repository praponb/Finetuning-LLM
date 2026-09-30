from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("start_date", "id")
w2 = Window.partitionBy("customer_id").orderBy("start_date_plus_months", "id").rowsBetween(-6, -1)
w3 = Window.partitionBy("plan").orderBy(F.col("monthly_fee_avg_6_prior").desc(), "id")
w4 = Window.partitionBy("plan").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("customer_id", "status").orderBy(F.col("start_date_plus_months").desc(), "id")
w6 = Window.partitionBy("status").orderBy("start_date_plus_months", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("status").orderBy(F.col("prev_monthly_fee").desc(), "id")
w_ordered = Window.partitionBy("plan").orderBy("monthly_fee_avg_6_prior", "id")
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.max("monthly_fee").alias("max_subscription_monthly_fee"))

valid = (
    df_subscriptions
    .filter((F.col("start_date") >= F.lit("2023-11-21").cast("date")) & F.col("customer_id").isNotNull() & F.col("customer_id").isin([3, 36]))
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "monthly_fee",
        "status",
        F.add_months("start_date", 6).alias("start_date_plus_months"),
    )
)

scored = (
    valid
    .select(
        "*",
        F.lag("monthly_fee", 1).over(w).alias("prev_monthly_fee"),
        F.avg("monthly_fee").over(w2).alias("monthly_fee_avg_6_prior"),
    )
)

with_dims = (
    scored
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "inner")
    .select("src.*", F.col("p.country"))
)

ranked = (
    with_dims
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_windows = (
    ranked
    .select(
        "*",
        F.sum("monthly_fee").over(w4).alias("cumulative_monthly_fee"),
        F.row_number().over(w5).alias("event_seq"),
        F.first("prev_monthly_fee", ignorenulls=True).over(w6).alias("first_known_prev_monthly_fee"),
        F.ntile(3).over(w7).alias("prev_monthly_fee_bucket"),
        F.lead("monthly_fee_avg_6_prior", 1).over(w_ordered).alias("next_monthly_fee_avg_6_prior"),
    )
)

with_dims_subscriptions = (
    with_windows
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.name"))
)

by_type = (
    with_dims_subscriptions
    .groupBy("customer_id")
    .pivot("status", ["ACTIVE", "EXPIRED", "CANCELLED"])
    .agg(F.avg("first_known_prev_monthly_fee").alias("total"))
)

result = (
    by_type
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "customer_id",
        "EXPIRED",
        "CANCELLED",
        F.col("max_subscription_monthly_fee"),
    )
    .orderBy(F.col("max_subscription_monthly_fee").desc(), "customer_id")
)
