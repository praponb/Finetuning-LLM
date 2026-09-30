from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(~F.col("account_type").isin(["SAVINGS", "CHECKING", "LOAN"]))
    .select("status", "opened_date")
    .unionByName(
        df_accounts
        .filter(F.col("balance").between(35000, 54000))
        .select("status", "opened_date")
    )
)
