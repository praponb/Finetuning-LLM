from pyspark.sql import functions as F

result = (
    df_accounts
    .select("status")
    .unionByName(
        df_shipments
        .filter(F.col("status").isin(["IN_TRANSIT", "LOST", "DELIVERED"]))
        .select("status")
    )
    .distinct()
)
