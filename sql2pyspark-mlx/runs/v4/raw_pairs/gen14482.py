from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "segment").orderBy(F.col("signup_date").desc_nulls_last(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.start_date").desc(), F.col("c.id").desc())
w3 = Window.orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("city").orderBy(F.col("age").desc(), "id")
w5 = Window.orderBy("subscription_monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_customers
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "email",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        (F.col("age") - F.col("credit_limit")).alias("age_minus_credit_limit"),
        F.last_day("signup_date").alias("signup_date_month_end"),
    )
)

latest = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

latest_match = (
    latest
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.start_date") <= F.col("s.signup_date_month_end")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.plan"),
        F.col("c.monthly_fee").alias("subscription_monthly_fee"),
    )
)

result = (
    latest_match
    .withColumn("running_age_minus_credit_limit", F.sum("age_minus_credit_limit").over(w3))
    .withColumn("age_bucket", F.ntile(3).over(w4))
    .withColumn("cumulative_credit_limit", F.sum("credit_limit").over(w5))
)
