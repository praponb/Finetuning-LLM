from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isin(["FROZEN", "CLOSED", "OPEN"]))
    .select("status")
    .unionByName(df_shipments.select("status"))
    .orderBy(F.col("status").desc())
)
