from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.country") == "US")
    .select(
        F.col("a.id"),
        F.col("a.opened_date"),
        F.col("a.status"),
        F.col("a.customer_id"),
        F.col("c.age"),
        F.col("c.credit_limit"),
        F.quarter(F.col("a.opened_date")).alias("opened_date_quarter"),
    )
)
