from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id")
w3 = Window.partitionBy("country").orderBy(F.col("started_at").desc(), "id")

enriched = (
    df_sessions
    .withColumn("first_known_country", F.first("country", ignorenulls=True).over(w))
    .withColumn("event_seq", F.row_number().over(w2))
    .withColumn("row_seq", F.row_number().over(w3))
)

result = (
    enriched
    .select("id", "customer_id", "device", "row_seq")
    .orderBy(F.col("row_seq").desc_nulls_last())
)
