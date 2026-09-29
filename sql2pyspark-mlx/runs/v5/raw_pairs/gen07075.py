from pyspark.sql import functions as F

result = (
    df_stores
    .select("opened_date")
    .union(df_shipments.select("delivered_date"))
)
