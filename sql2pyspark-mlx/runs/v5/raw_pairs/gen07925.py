from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.filter(~F.col("segment").isin(["SMB", "Corporate"])).alias("c"), F.col("a.customer_id") == F.col("c.id"), "left_semi")
    .filter(F.col("a.customer_id").isin([19, 33]))
    .select(
        F.col("a.id"),
        F.col("a.account_type"),
        F.col("a.balance"),
        F.col("a.customer_id"),
    )
)
