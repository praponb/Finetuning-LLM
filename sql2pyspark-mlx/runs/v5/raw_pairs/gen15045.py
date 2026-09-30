from pyspark.sql import functions as F

result = (
    df_accounts
    .select("status")
    .subtract(
        df_shipments
        .filter(F.col("shipped_date") >= "2023-07-01")
        .select("status")
    )
    .orderBy("status")
)
