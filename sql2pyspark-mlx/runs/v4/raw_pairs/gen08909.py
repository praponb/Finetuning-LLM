from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date") > "2025-01-25")
    .select(
        "id",
        "balance",
        F.add_months("opened_date", 12).alias("opened_date_plus_months"),
    )
    .orderBy(F.col("id").desc())
    .limit(50)
)
