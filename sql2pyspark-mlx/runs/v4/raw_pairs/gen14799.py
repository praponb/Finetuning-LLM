from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date") < "2024-12-01")
    .select(
        "customer_id",
        F.datediff(F.current_date(), "opened_date").alias("days_since_opened_date"),
    )
)
