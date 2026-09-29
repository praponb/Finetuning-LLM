from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "browser").orderBy("started_at", "id")
w_hist = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("started_at").desc(), "id")

result = (
    df_sessions
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("first_known_browser", F.first("browser", ignorenulls=True).over(w_hist))
    .withColumn("row_seq", F.row_number().over(w3))
)
