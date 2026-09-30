from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("device").orderBy(F.col("started_at").desc(), "id")
w_hist = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("customer_id").orderBy("started_at", "id")

result = (
    df_sessions
    .filter(F.col("country").isin(["DE", "JP", "US"]))
    .withColumn("seq_no", F.row_number().over(w_recent))
    .withColumn("row_seq", F.row_number().over(w_recent))
    .withColumn("first_known_country", F.first("country", ignorenulls=True).over(w_hist))
    .withColumn("seq_no_2", F.row_number().over(w_part))
)
