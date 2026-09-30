from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("end_date").desc_nulls_last(), "id")

base = (
    df_subscriptions
    .filter(F.col("monthly_fee") > 180)
    .withColumn("end_date_label", F.date_format("end_date", "yyyy-MM-dd"))
    .withColumn("status_lower", F.lower("status"))
)

result = (
    base
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)
