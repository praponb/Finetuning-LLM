from pyspark.sql import functions as F

kept = (
    df_customers
    .alias("s")
    .join(df_subscriptions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.month(F.col("ch.start_date")).isin([2, 3, 7])), "left_semi")
)

result = (
    kept
    .select(
        "id",
        "name",
        "country",
        "city",
        "segment",
        "signup_date",
        "credit_limit",
        F.when(F.col("segment").rlike("^(Corporate|Consumer)$"), "PRIORITY").when(F.col("segment").rlike("^(SMB)$"), "GROUP_B").otherwise("GROUP_A").alias("segment_zone"),
    )
    .orderBy(F.col("segment_zone").desc_nulls_first())
)
