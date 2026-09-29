from pyspark.sql import functions as F

result = (
    df_accounts
    .select("opened_date")
    .intersect(df_shipments.filter(F.col("carrier") == "UPS").select("shipped_date"))
    .orderBy(F.col("opened_date").desc())
)
