from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isNull())
    .select("opened_date")
    .union(df_shipments.select("shipped_date"))
    .distinct()
)
