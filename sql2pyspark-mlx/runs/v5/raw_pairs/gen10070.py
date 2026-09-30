from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("end_date", "id").rowsBetween(-2, -1)
w3 = Window.partitionBy("customer_id").orderBy(F.col("start_date").desc(), "id")
w4 = Window.partitionBy("customer_id").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("status")
w6 = Window.partitionBy("plan", "status")
w7 = Window.partitionBy("customer_id").orderBy("signup_date", "id")
w8 = Window.partitionBy("status", "plan").orderBy("age", "id")

base = (
    df_subscriptions
    .filter((F.col("end_date") > F.to_date(F.lit("2023-04-13"))) & F.col("customer_id").isNotNull())
    .select(
        "*",
        F.lower("status").alias("status_lower"),
        F.when(F.col("monthly_fee") >= 241, "High").when(F.col("monthly_fee") >= 44, "Medium").otherwise("Low").alias("monthly_fee_band"),
    )
)

enriched = (
    base
    .select(
        "*",
        F.first("monthly_fee", ignorenulls=True).over(w).alias("first_known_monthly_fee"),
        F.avg("monthly_fee").over(w2).alias("monthly_fee_avg_2_prior"),
        F.row_number().over(w3).alias("seq_no"),
        F.sum("monthly_fee").over(w4).alias("running_monthly_fee"),
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w5)), 4).alias("monthly_fee_share"),
    )
)

enriched_subscriptions = (
    enriched
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .select("s.*", F.col("c.signup_date"), F.col("c.age"))
)

enriched_subscriptions_2 = (
    enriched_subscriptions
    .select(
        "id",
        "end_date",
        "monthly_fee",
        "status",
        "monthly_fee_band",
        "monthly_fee_avg_2_prior",
        "signup_date",
        F.sum("seq_no").over(w6).alias("plan_sum_seq_no"),
        F.lag("monthly_fee_share", 1).over(w7).alias("prev_monthly_fee_share"),
        F.row_number().over(w8).alias("seq_no_2"),
    )
)

result = (
    enriched_subscriptions_2
    .select(
        "id",
        "end_date",
        "monthly_fee",
        "status",
        "monthly_fee_avg_2_prior",
        "signup_date",
        "prev_monthly_fee_share",
    )
    .orderBy(F.col("monthly_fee_avg_2_prior").desc(), F.col("end_date").desc())
)
