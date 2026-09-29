from pyspark.sql import functions as F

customer_totals = (
    df_accounts
    .filter(F.col("status").isin(["FROZEN", "OPEN"]))
    .groupBy("customer_id")
    .agg(
        F.count("*").alias("row_count"),
        F.sum(F.when(F.col("account_type") == "LOAN", 1).otherwise(0)).alias("loan_count"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("ct"), F.col("c.id") == F.col("ct.customer_id"), "inner")
    .filter(F.col("ct.row_count") > 6)
    .select(
        F.col("c.id"),
        F.col("c.country"),
        F.col("ct.row_count"),
        F.col("ct.loan_count"),
    )
    .orderBy("country", "id")
    .limit(3)
)
