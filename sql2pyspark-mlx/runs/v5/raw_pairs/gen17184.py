from pyspark.sql import functions as F

result = (
    df_accounts
    .select("opened_date")
    .intersect(df_shipments.select("delivered_date"))
)
