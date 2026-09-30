from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.filter(F.col("age").between(43, 67)).alias("c"), F.col("a.customer_id") == F.col("c.id"), "left_semi")
    .filter(F.col("a.opened_date").between("2023-08-01", "2024-08-09"))
    .select(F.col("a.id"), F.col("a.opened_date"))
)
