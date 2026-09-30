from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("plan").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist_2 = Window.partitionBy("status").orderBy(F.col("monthly_fee").desc(), "id")

result = (
    df_subscriptions
    .withColumn("last_known_monthly_fee", F.last("monthly_fee", ignorenulls=True).over(w_hist))
    .withColumn("seq_no", F.row_number().over(w_hist_2))
    .withColumn("row_seq", F.row_number().over(w_hist_2))
)
