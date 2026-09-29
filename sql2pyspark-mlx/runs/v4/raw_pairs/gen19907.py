from pyspark.sql import functions as F

joined = (
    df_subscriptions
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.country"))
)

result = (
    joined
    .select(
        "id",
        "customer_id",
        "end_date",
        "monthly_fee",
        "status",
        "country",
        F.when(F.col("country").rlike("^(SG|US)$"), "GROUP_A").otherwise("PRIORITY").alias("country_zone"),
    )
    .orderBy(F.col("country").desc(), F.col("end_date").desc())
)
