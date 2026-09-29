from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter(F.col("c.country").isin(["US", "TH"]))
    .groupBy(
        F.quarter(F.col("c.signup_date")).alias("signup_quarter"),
        F.date_trunc("MONTH", F.col("c.signup_date")).alias("signup_month"),
    )
    .agg(
        F.countDistinct(F.col("s.monthly_fee")).alias("unique_statuss"),
        F.count(F.col("s.monthly_fee")).alias("monthly_fee_count"),
    )
    .orderBy("monthly_fee_count", F.col("signup_quarter").desc())
)
