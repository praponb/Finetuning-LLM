from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "right")
    .filter(F.col("a.customer_id").isNotNull())
    .select(
        F.col("a.status"),
        F.col("c.country"),
        F.col("c.segment"),
        F.col("c.city"),
        (F.col("c.age") - F.col("c.credit_limit")).alias("age_minus_credit_limit"),
    )
    .orderBy(F.col("age_minus_credit_limit").desc())
)
