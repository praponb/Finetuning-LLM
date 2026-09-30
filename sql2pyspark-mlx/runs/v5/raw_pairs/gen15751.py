from pyspark.sql import functions as F

result = (
    df_accounts
    .select("customer_id", "status")
    .subtract(
        df_accounts
        .filter(F.col("balance") < 78000)
        .select("customer_id", "status")
    )
)
