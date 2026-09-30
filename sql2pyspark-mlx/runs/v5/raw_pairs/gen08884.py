from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("browser", "customer_id").orderBy(F.col("started_at").desc(), "id")

result = (
    df_sessions
    .withColumn("last_known_browser", F.last("browser", ignorenulls=True).over(w))
    .withColumn("first_known_country", F.first("country", ignorenulls=True).over(w))
    .withColumn("seq_no", F.row_number().over(w2))
)
