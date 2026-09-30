from pyspark.sql import functions as F

eligible = (
    df_customers
    .alias("s")
    .join(df_subscriptions.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.end_date") >= F.col("s.signup_date")), "left_semi")
)

result = (
    eligible
    .select(
        "id",
        "name",
        "country",
        "city",
        "segment",
        "is_active",
        "credit_limit",
        "age",
        F.when(F.col("name").rlike("^(Jack Taylor|  Carol Diaz)$"), "EXTENDED").when(F.col("name").rlike("^(Alice Wong|bob smith)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("name_zone"),
    )
    .orderBy(F.col("credit_limit").asc_nulls_first())
)
