from pyspark.sql import functions as F

result = (
    df_accounts
    .select(
        "id",
        "customer_id",
        "opened_date",
        F.round((F.col("balance") * 0.9), 2).alias("balance_scaled"),
    )
)
