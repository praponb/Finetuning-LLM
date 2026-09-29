from pyspark.sql import functions as F

result = (
    df_accounts
    .select("customer_id")
    .subtract(
        df_accounts
        .filter(F.col("status") == "OPEN")
        .select("customer_id")
    )
)
