from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("amount") <= 1250) & F.hour("paid_at").between(6, 11))
    .withColumn("status_upper", F.upper("status"))
    .withColumn("paid_at_day", F.col("paid_at").cast("date"))
    .orderBy("status_upper", "id")
    .limit(10)
)
