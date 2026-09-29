from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isin([14, 19, 21, 28]))
    .select("id", "customer_id")
    .unionByName(
        df_accounts
        .filter(F.col("opened_date") <= "2025-09-02")
        .select("id", "customer_id")
    )
    .orderBy("customer_id")
)
