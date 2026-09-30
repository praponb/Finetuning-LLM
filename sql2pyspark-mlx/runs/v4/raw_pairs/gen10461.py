from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type") == "LOAN")
    .select(
        "status",
        F.date_format("opened_date", "MMM yyyy").alias("opened_date_label"),
    )
    .orderBy("status")
)
