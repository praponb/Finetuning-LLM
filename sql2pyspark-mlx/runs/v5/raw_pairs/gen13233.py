from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type").isin(["SAVINGS", "LOAN", "CHECKING"]))
    .select("status")
    .subtract(df_shipments.filter(F.col("carrier") == "FedEx").select("status"))
)
