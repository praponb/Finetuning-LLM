from pyspark.sql import functions as F

base = (
    df_accounts
    .filter(F.col("opened_date") > F.lit("2023-11-07").cast("date"))
    .select(
        "id",
        "customer_id",
        "account_type",
        "opened_date",
        "status",
        F.last_day("opened_date").alias("opened_date_month_end"),
    )
)

joined = (
    base
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.city"), F.col("b.signup_date"))
)

joined_accounts = (
    joined
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .select("a.*", F.col("c.email"))
)

result = (
    joined_accounts
    .select(
        "id",
        "opened_date",
        "status",
        "city",
        "signup_date",
        "email",
        F.when(F.col("city").rlike("^(Berlin|Singapore)$"), "GROUP_A").when(F.col("city").rlike("^(Bangkok|Chiang Mai)$"), "CORE").otherwise("EXTENDED").alias("city_zone"),
    )
    .orderBy("city_zone")
)
