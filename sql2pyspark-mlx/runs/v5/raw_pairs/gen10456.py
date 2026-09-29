from pyspark.sql import functions as F

result = (
    df_shipments
    .select("id", "delivered_date")
    .orderBy("id", "delivered_date")
)
