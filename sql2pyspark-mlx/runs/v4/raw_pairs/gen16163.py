from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date") >= "2024-04-23")
    .select("status")
    .intersect(df_shipments.select("status"))
    .orderBy("status")
)
