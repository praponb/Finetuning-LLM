from pyspark.sql import functions as F

eligible = (
    df_customers
    .alias("s")
    .join(df_subscriptions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.end_date") <= F.col("s.signup_date")), "left_semi")
)

result = (
    eligible
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        F.when(F.col("country").rlike("^(SG|TH)$"), "OTHER_ZONE").when(F.col("country").rlike("^(US|JP)$"), "EXTENDED").otherwise("CORE").alias("country_zone"),
    )
)
