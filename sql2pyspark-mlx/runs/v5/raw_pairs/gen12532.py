from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isNotNull() & F.col("customer_id").isin([13, 34, 35, 38]) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "account_type",
        "opened_date",
        "balance",
        F.date_trunc("month", "opened_date").alias("opened_date_period"),
        F.date_format("opened_date", "yyyy-MM").alias("opened_date_label"),
    )
    .orderBy("opened_date_label", "account_type")
)
