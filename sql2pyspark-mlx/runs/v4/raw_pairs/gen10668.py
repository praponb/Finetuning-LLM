from pyspark.sql import functions as F

customer_summary = (
    df_accounts
    .filter(F.col("customer_id").isin([31, 39]))
    .groupBy("customer_id")
    .agg(
        F.min("opened_date").alias("first_opened_date"),
        F.round(((100 * F.sum(F.when(F.col("account_type") == "LOAN", 1).otherwise(0))) / F.count("*")), 1).alias("pct_loan"),
    )
)

result = (
    df_customers.alias("customers")
    .join(customer_summary.alias("customer_summary"), F.col("customers.id") == F.col("customer_summary.customer_id"), "inner")
    .filter(F.col("customer_summary.pct_loan") >= 33)
    .select(
        F.col("customers.is_active"),
        F.col("customer_summary.first_opened_date"),
        F.col("customer_summary.pct_loan"),
    )
)
