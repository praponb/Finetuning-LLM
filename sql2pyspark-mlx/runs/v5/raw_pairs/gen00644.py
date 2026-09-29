from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date") > "2025-04-01")
    .select("status")
    .unionByName(
        df_payments
        .filter(F.col("amount").between(430, 1190))
        .select("status")
    )
    .distinct()
)
