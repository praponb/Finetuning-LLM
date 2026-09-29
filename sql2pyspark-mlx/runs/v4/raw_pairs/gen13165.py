from pyspark.sql import functions as F

max_delivered_date = df_shipments.filter(F.year("shipped_date") >= 2024).agg(F.max("delivered_date")).first()[0]

result = (
    df_shipments
    .filter(F.col("delivered_date") == max_delivered_date)
    .select("id", "delivered_date")
    .orderBy("delivered_date")
)
