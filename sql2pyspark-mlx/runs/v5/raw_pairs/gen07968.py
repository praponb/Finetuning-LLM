from pyspark.sql import functions as F

result = (
    df_shipments
    .withColumn("delivered_date_plus_months", F.add_months("delivered_date", 3))
    .withColumn("shipped_date_minus_55d", F.date_sub("shipped_date", 55))
)
