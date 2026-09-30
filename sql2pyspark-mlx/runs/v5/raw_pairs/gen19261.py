from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_customers.alias("customers"), F.col("accounts.customer_id") == F.col("customers.id"), "inner")
    .filter(F.col("customers.name").like("%Emm%") & (F.col("customers.city") == "New York"))
    .select(
        F.col("accounts.id"),
        F.col("accounts.opened_date"),
        F.col("accounts.balance"),
        F.col("accounts.customer_id"),
        F.col("customers.signup_date"),
        F.col("customers.name"),
    )
    .orderBy(F.col("id").desc())
)
