from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("status") == "FROZEN") & (F.col("opened_date").between("2024-06-01", "2025-07-01") | F.col("customer_id").isNull()))
    .select(
        "id",
        "opened_date",
        F.date_format("opened_date", "yyyy-MM-dd").alias("opened_date_label"),
    )
    .orderBy(F.col("opened_date_label").desc())
)
