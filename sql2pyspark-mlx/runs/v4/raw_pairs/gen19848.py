from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isNotNull())
    .select("status")
    .subtract(
        df_accounts
        .filter(F.col("status").isin(["OPEN", "CLOSED", "FROZEN"]))
        .select("status")
    )
)
