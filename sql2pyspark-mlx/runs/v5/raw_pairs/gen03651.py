from pyspark.sql import functions as F

customer_stats = (
    df_accounts
    .filter(F.col("account_type") == "CREDIT")
    .groupBy("customer_id")
    .agg(F.min("opened_date").alias("first_opened_date"))
)

result = (
    df_customers.alias("customers")
    .join(customer_stats.alias("customer_stats"), F.col("customers.id") == F.col("customer_stats.customer_id"), "inner")
    .select(
        F.col("customers.id"),
        F.col("customers.segment"),
        F.col("customers.name"),
        F.col("customer_stats.first_opened_date"),
    )
    .orderBy("name")
)
