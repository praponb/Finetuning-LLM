from pyspark.sql import functions as F

result = (
    df_accounts
    .select(
        "customer_id",
        F.round("balance", 0).alias("balance_rounded"),
        F.when(F.col("status") == "OPEN", 1).otherwise(0).alias("is_open"),
    )
)
