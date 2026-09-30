from pyspark.sql import functions as F

max_delivered_date = df_shipments.agg(F.max("delivered_date")).first()[0]

result = (
    df_shipments
    .filter(F.col("delivered_date") == max_delivered_date)
    .select("id", "delivered_date", "order_id", "cost")
    .orderBy("order_id")
)
