from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.opened_date").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("account_account_type", "country").orderBy("city_min_credit_limit", "id")
w5 = Window.partitionBy("account_account_type").orderBy("city_min_credit_limit", "id")
w6 = Window.orderBy("city_min_credit_limit", "id").rowsBetween(-2, Window.currentRow)
w7 = Window.partitionBy("country", "account_account_type").orderBy(F.col("city_min_credit_limit").desc_nulls_last())
w8 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.monthly_fee").desc(), F.col("c.id").desc())
w9 = Window.orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("subscription_plan").orderBy("city_min_credit_limit_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.partitionBy("subscription_plan", "status")

with_windows = (
    df_customers
    .filter(F.col("segment") == "SMB")
    .select(
        "id",
        "email",
        "country",
        "signup_date",
        "age",
        F.last("city", ignorenulls=True).over(w).alias("last_known_city"),
        F.min("credit_limit").over(w2).alias("city_min_credit_limit"),
    )
)

with_last = (
    with_windows
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.account_type").alias("account_account_type"))
)

enriched = (
    with_last
    .select(
        "*",
        F.ntile(10).over(w4).alias("city_min_credit_limit_bucket"),
        F.lead("city_min_credit_limit", 1).over(w5).alias("next_city_min_credit_limit"),
        F.avg("city_min_credit_limit").over(w6).alias("city_min_credit_limit_avg_2"),
    )
)

deduped = (
    enriched
    .withColumn("dedup_rank", F.rank().over(w7))
    .filter(F.col("dedup_rank") == 1)
)

with_latest = (
    deduped
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.email",
        "s.country",
        "s.signup_date",
        "s.age",
        "s.city_min_credit_limit",
        "s.account_account_type",
        "s.city_min_credit_limit_bucket",
        "s.next_city_min_credit_limit",
        F.col("c.plan").alias("subscription_plan"),
        F.col("c.status"),
    )
)

enriched_customers = (
    with_latest
    .select(
        "*",
        F.sum("city_min_credit_limit_bucket").over(w9).alias("cumulative_city_min_credit_limit_bucket"),
        F.sum("city_min_credit_limit").over(w_recent).alias("cumulative_city_min_credit_limit"),
        F.round((F.col("next_city_min_credit_limit") / F.sum("next_city_min_credit_limit").over(w11)), 4).alias("next_city_min_credit_limit_share"),
    )
)

result = (
    enriched_customers
    .select(
        "id",
        "email",
        "country",
        "signup_date",
        "age",
        "city_min_credit_limit",
        "account_account_type",
        "city_min_credit_limit_bucket",
        "next_city_min_credit_limit",
        F.when(F.col("account_account_type").rlike("^(SAVINGS|LOAN)$"), "GROUP_A").otherwise("CORE").alias("account_account_type_zone"),
    )
)
