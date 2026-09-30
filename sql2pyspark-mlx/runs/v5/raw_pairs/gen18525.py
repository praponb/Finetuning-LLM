from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") <= 28000)
    .select(
        "id",
        "balance",
        "opened_date",
        "customer_id",
        F.month("opened_date").alias("opened_date_month"),
        F.length("status").alias("status_length"),
    )
    .orderBy(F.col("status_length").desc())
)
