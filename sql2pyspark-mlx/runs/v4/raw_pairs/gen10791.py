from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id") == 23)
    .withColumn("days_since_delivered_date", F.datediff(F.current_date(), "delivered_date"))
    .withColumn("status_upper", F.upper("status"))
)
