from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date") > "2024-03-01")
    .select(
        "id",
        "balance",
        F.date_format("opened_date", "yyyy-MM-dd").alias("opened_date_label"),
    )
)
