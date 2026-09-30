from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .filter((F.col("c.segment") == "SMB") & (F.col("a.opened_date") <= "2023-07-01"))
    .select(
        F.col("a.id"),
        F.col("a.customer_id"),
        F.col("a.opened_date"),
        F.col("c.city"),
        F.col("c.age"),
    )
    .orderBy(F.col("id").desc())
)
