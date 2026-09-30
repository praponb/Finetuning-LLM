from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type").isin(["CREDIT", "LOAN"]))
    .select(
        "id",
        "customer_id",
        "balance",
        "status",
        F.add_months("opened_date", 6).alias("opened_date_plus_months"),
    )
    .orderBy(F.col("status").desc())
)
