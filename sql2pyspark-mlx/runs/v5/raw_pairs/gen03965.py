from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_accounts.alias("a"), F.col("c.id") == F.col("a.customer_id"), "left")
    .filter(~((F.col("c.segment") != "SMB") & (F.col("c.signup_date") <= "2025-05-01")))
    .select(
        F.col("c.segment"),
        F.col("c.is_active"),
        F.col("c.email"),
        F.col("a.customer_id"),
        F.when(F.col("c.city").isin(["New York", "London"]), "Group A").otherwise("Other").alias("city_group"),
    )
    .orderBy(F.col("city_group").desc())
)
