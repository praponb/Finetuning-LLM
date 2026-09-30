from pyspark.sql import functions as F

customer_summary = (
    df_accounts
    .groupBy("customer_id")
    .agg(
        F.countDistinct("balance").alias("unique_customers"),
        F.min("opened_date").alias("first_opened_date"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "inner")
    .filter(F.col("cs.unique_customers") > 8)
    .select(
        F.col("c.id"),
        F.col("c.credit_limit"),
        F.col("c.email"),
        F.col("cs.unique_customers"),
        F.col("cs.first_opened_date"),
    )
)
