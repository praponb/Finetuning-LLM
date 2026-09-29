from pyspark.sql import functions as F

joined = (
    df_subscriptions
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id").eqNullSafe(F.col("p.id")), "left")
    .select("e.*", F.col("p.segment"), F.col("p.email"), F.col("p.city"))
)

result = (
    joined
    .select(
        "id",
        "customer_id",
        "start_date",
        "end_date",
        "monthly_fee",
        "segment",
        "email",
        "city",
        F.when(F.col("segment").rlike("^(Corporate|Consumer)$"), "GROUP_A").when(F.col("segment").rlike("^(SMB)$"), "PRIORITY").otherwise("GROUP_B").alias("segment_zone"),
    )
    .orderBy(F.col("segment").asc_nulls_last())
)
