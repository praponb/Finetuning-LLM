from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("device").orderBy(F.col("started_at").desc(), "id")
w_running = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("country").orderBy("started_at", "id")
w5 = Window.orderBy("started_at", "id")

result = (
    df_sessions
    .select(
        "*",
        F.first("country", ignorenulls=True).over(w).alias("first_known_country"),
        F.row_number().over(w2).alias("seq_no"),
        F.last("country", ignorenulls=True).over(w_running).alias("last_known_country"),
        F.row_number().over(w4).alias("event_seq"),
        F.row_number().over(w5).alias("seq_no_2"),
    )
)
