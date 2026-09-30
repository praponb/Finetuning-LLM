from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance").between(28000, 36000))
    .select("id", "customer_id")
    .unionByName(
        df_accounts
        .filter(F.col("opened_date") <= "2025-09-01")
        .select("id", "customer_id")
    )
    .orderBy("customer_id")
)
