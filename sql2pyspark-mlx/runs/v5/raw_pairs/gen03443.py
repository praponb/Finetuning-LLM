from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.start_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("subscription_plan").orderBy("age", "id")
w3 = Window.partitionBy("segment").orderBy("subscription_start_date", "id").rowsBetween(-5, Window.currentRow)
w4 = Window.partitionBy("segment")
w5 = Window.orderBy("signup_date", "id")
w6 = Window.partitionBy("city").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_customers
    .filter(F.col("signup_date") > F.to_date(F.lit("2023-07-12")))
    .select(
        "*",
        F.lower("email").alias("email_lower"),
        F.length("email").alias("email_length"),
    )
)

latest_match = (
    base
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.start_date") <= F.col("s.signup_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.plan").alias("subscription_plan"),
        F.col("c.start_date").alias("subscription_start_date"),
    )
)

base_customers = (
    latest_match
    .filter((F.col("signup_date") >= F.lit("2023-08-12").cast("date")) & (F.col("email_length") <= 77))
    .select(
        "*",
        F.coalesce("credit_limit", F.lit(0)).alias("credit_limit_filled"),
        F.upper(F.trim("name")).alias("name_normalized"),
        F.coalesce("subscription_plan", F.lit("N/A")).alias("subscription_plan_filled"),
    )
)

scored = (
    base_customers
    .select(
        "id",
        "name",
        "email",
        "country",
        "segment",
        "credit_limit",
        "age",
        "email_lower",
        "subscription_start_date",
        "subscription_plan_filled",
        F.row_number().over(w2).alias("event_seq"),
        F.avg("email_length").over(w3).alias("email_length_avg_5"),
        F.corr("credit_limit", "age").over(w4).alias("corr_credit_limit_age"),
        F.lag("age", 1).over(w5).alias("prev_age"),
        F.last("subscription_plan", ignorenulls=True).over(w6).alias("last_known_subscription_plan"),
    )
)

result = (
    scored
    .select(
        "id",
        "name",
        "email",
        "country",
        "segment",
        "credit_limit",
        "email_lower",
        "subscription_plan_filled",
        "email_length_avg_5",
        F.when(F.col("name").rlike("^(Jack Taylor|bob smith)$"), "EXTENDED").when(F.col("name").rlike("^(Alice Wong|  Carol Diaz)$"), "GROUP_A").otherwise("OTHER_ZONE").alias("name_zone"),
    )
    .orderBy(F.col("email_length_avg_5").desc_nulls_first())
)
