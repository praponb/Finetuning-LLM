from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(((F.col("account_type") == "CHECKING") & F.col("customer_id").isNotNull()) | (F.col("account_type") != "SAVINGS"))
    .groupBy("status")
    .agg(
        F.min("opened_date").alias("first_opened_date"),
        F.max("balance").alias("max_balance"),
    )
)
