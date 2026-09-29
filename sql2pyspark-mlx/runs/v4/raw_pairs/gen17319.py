from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("end_date_plus_months", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy("start_date", "id")
w3 = Window.partitionBy("customer_id").orderBy("monthly_fee", "id").rowsBetween(-5, Window.currentRow)

base = (
    df_subscriptions
    .filter((F.col("end_date") >= F.to_date(F.lit("2023-09-14"))) & F.col("customer_id").isNotNull())
    .withColumn("end_date_plus_months", F.add_months("end_date", 12))
)

result = (
    base
    .withColumn("cumulative_monthly_fee", F.sum("monthly_fee").over(w))
    .withColumn("event_seq", F.row_number().over(w2))
    .withColumn("monthly_fee_avg_5", F.avg("monthly_fee").over(w3))
)
