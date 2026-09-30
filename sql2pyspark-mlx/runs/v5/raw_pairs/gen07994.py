from pyspark.sql import functions as F

result = (
    df_shipments
    .select("delivered_date")
    .union(df_accounts.select("opened_date"))
    .distinct()
)
