from pyspark.sql import functions as F

base = (
    df_accounts
    .filter(F.col("customer_id").isin([13, 19, 24]))
    .select(
        "id",
        "account_type",
        "balance",
        F.quarter("opened_date").alias("opened_date_quarter"),
        F.date_format("opened_date", "MMM yyyy").alias("opened_date_label"),
    )
)

result = (
    base
    .select("id")
    .intersectAll(
        df_accounts
        .filter(F.col("account_type").isin(["CREDIT", "CHECKING", "LOAN"]))
        .select("id")
    )
)
