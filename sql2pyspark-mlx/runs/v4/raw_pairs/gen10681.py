from pyspark.sql import functions as F

kept = (
    df_customers
    .alias("s")
    .join(df_subscriptions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.start_date") >= F.col("s.signup_date")), "left_anti")
)

result = (
    kept
    .select(
        "id",
        "name",
        "email",
        "segment",
        "signup_date",
        "credit_limit",
        "age",
        F.when(F.col("name").rlike("^(Ivy Chen|Kanya Srisuk)$"), "GROUP_A").when(F.col("name").rlike("^(Alice Wong|bob smith)$"), "OTHER_ZONE").otherwise("CORE").alias("name_zone"),
    )
    .orderBy("age", "name_zone")
)
