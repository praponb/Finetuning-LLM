from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.balance").desc(), F.col("c.id").desc())
w_grp = Window.partitionBy("country").orderBy("age", "id")
w3 = Window.partitionBy("city").orderBy("signup_date", "id").rowsBetween(-3, Window.currentRow)
w_part = Window.partitionBy("country").orderBy("next_age", "id").rowsBetween(-3, Window.currentRow)
w5 = Window.partitionBy("country", "city").orderBy(F.col("balance").desc(), "id")
w6 = Window.partitionBy("country")
w7 = Window.partitionBy("country").orderBy("last4_sum_prev_balance", "id")

with_latest = (
    df_customers
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.name",
        "s.email",
        "s.country",
        "s.city",
        "s.signup_date",
        "s.is_active",
        "s.age",
        F.col("c.balance"),
    )
)

scored = (
    with_latest
    .filter(F.col("city").isNotNull())
    .select(
        "*",
        F.lag("balance").over(w_grp).alias("prev_balance"),
        F.lead("balance").over(w_grp).alias("next_balance"),
        F.lead("age").over(w_grp).alias("next_age"),
        F.row_number().over(w_grp).alias("event_seq"),
        F.avg("age").over(w3).alias("age_avg_3"),
    )
)

kept = (
    scored
    .alias("s")
    .join(df_accounts.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.balance") <= 59000), "left_semi")
)

metrics = (
    kept
    .withColumn("last4_sum_prev_balance", F.sum("prev_balance").over(w_part))
    .withColumn("last4_max_prev_balance", F.max("prev_balance").over(w_part))
    .withColumn("last4_avg_prev_balance", F.avg("prev_balance").over(w_part))
    .withColumn("last4_count_prev_balance", F.count("prev_balance").over(w_part))
    .withColumn("row_seq", F.row_number().over(w5))
)

kept_customers = (
    metrics
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.order_date") < F.col("s.signup_date")), "left_semi")
)

scored_customers = (
    kept_customers
    .select(
        "id",
        "email",
        "signup_date",
        "is_active",
        "balance",
        "next_balance",
        "next_age",
        "event_seq",
        "age_avg_3",
        "last4_sum_prev_balance",
        "last4_avg_prev_balance",
        F.round((F.col("next_age") / F.sum("next_age").over(w6)), 4).alias("next_age_share"),
        F.lead("event_seq", 1, 0).over(w7).alias("next_event_seq"),
    )
)

valid = (
    scored_customers
    .filter(F.col("next_age_share") < 0.41)
    .withColumn("is_active_label", F.when(F.col("is_active"), "Yes").otherwise("No"))
)

result = (
    valid
    .select(
        "id",
        "email",
        "signup_date",
        "is_active",
        "balance",
        "event_seq",
        "age_avg_3",
        "last4_sum_prev_balance",
        "last4_avg_prev_balance",
    )
    .orderBy(F.col("balance").asc_nulls_last())
)
