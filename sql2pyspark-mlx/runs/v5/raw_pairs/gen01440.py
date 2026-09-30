from pyspark.sql import functions as F

by_dimension = (
    df_subscriptions
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "inner")
    .cube(F.col("p.city").alias("city"), F.col("s.status").alias("status"))
    .agg(
        F.count("*").alias("rows_total"),
        F.sum(F.col("s.monthly_fee")).alias("total_monthly_fee"),
    )
)

result = (
    by_dimension
    .select(
        "city",
        "status",
        "rows_total",
        "total_monthly_fee",
        F.when(F.col("status").rlike("^(EXPIRED|ACTIVE)$"), "CORE").when(F.col("status").rlike("^(CANCELLED|PAUSED)$"), "EXTENDED").otherwise("GROUP_B").alias("status_zone"),
    )
    .orderBy(F.col("total_monthly_fee").desc_nulls_first())
)
