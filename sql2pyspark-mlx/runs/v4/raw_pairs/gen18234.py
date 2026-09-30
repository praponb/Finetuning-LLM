from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "device").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id")
w3 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("customer_id").orderBy(F.col("started_at").desc(), "id")
w5 = Window.partitionBy("country").orderBy(F.col("started_at").desc(), "id")

metrics = (
    df_sessions
    .select(
        "*",
        F.row_number().over(w).alias("event_seq"),
        F.row_number().over(w2).alias("row_seq"),
        F.last("country", ignorenulls=True).over(w3).alias("last_known_country"),
        F.row_number().over(w4).alias("event_seq_2"),
        F.row_number().over(w5).alias("event_seq_3"),
    )
)

result = metrics.select("id", "device", "started_at", "row_seq", "event_seq_3")
