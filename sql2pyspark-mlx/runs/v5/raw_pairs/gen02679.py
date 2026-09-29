from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "plan").orderBy(F.col("start_date").desc_nulls_last(), "id")

deduped = (
    df_subscriptions
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "plan", "monthly_fee", "status")
)

result = (
    deduped
    .select("id", "monthly_fee", "status")
    .orderBy(F.col("monthly_fee").asc_nulls_last())
)
