from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("age", "id")
w_part = Window.partitionBy("country").orderBy("credit_limit", "id").rowsBetween(-5, Window.currentRow)

result = (
    df_customers
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("age_avg_5", F.avg("age").over(w_part))
)
