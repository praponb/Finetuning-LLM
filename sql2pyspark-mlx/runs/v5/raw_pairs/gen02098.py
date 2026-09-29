from pyspark.sql import functions as F

result = (
    df_shipments
    .withColumn("delivered_date_label", F.date_format("delivered_date", "yyyy-MM"))
)
