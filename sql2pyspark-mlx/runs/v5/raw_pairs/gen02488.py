from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date") <= "2023-08-20")
    .select(
        "id",
        "account_type",
        "balance",
        F.year("opened_date").alias("opened_date_year"),
    )
    .orderBy("opened_date_year", "id")
    .limit(20)
)
