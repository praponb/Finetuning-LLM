from pyspark.sql import functions as F

result = (
    df_accounts
    .select("status")
    .unionByName(
        df_shipments
        .filter(F.col("shipped_date") <= "2023-05-01")
        .select("status")
    )
)
