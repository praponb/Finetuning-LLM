from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.city").isin(["Singapore", "Berlin", "Chiang Mai"]))
    .select(
        F.col("a.id"),
        F.col("a.opened_date"),
        F.col("c.segment"),
        F.col("c.country"),
        F.col("c.name"),
        F.datediff(F.current_date(), F.col("c.signup_date")).alias("days_since_signup_date"),
    )
    .orderBy("segment")
)
