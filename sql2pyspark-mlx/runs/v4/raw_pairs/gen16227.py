from pyspark.sql import functions as F

excluded_customers = df_customers.filter(F.length("name") < 6).select("id")

result = (
    df_accounts.alias("accounts")
    .join(excluded_customers.alias("customers"), F.col("accounts.customer_id") == F.col("customers.id"), "left_anti")
    .filter(F.col("accounts.customer_id").isNotNull() | F.lit(excluded_customers.isEmpty()))
    .filter(F.col("accounts.customer_id").isNull())
    .select(F.col("accounts.id"), F.col("accounts.customer_id"))
    .orderBy(F.col("customer_id").desc(), "id")
    .limit(1)
)
