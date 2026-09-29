from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id", "country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("browser", "country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("started_at").desc(), "id")

scored = (
    df_sessions
    .select(
        "*",
        F.row_number().over(w_running).alias("seq_no"),
        F.last("browser", ignorenulls=True).over(w2).alias("last_known_browser"),
        F.last("browser", ignorenulls=True).over(w3).alias("last_known_browser_2"),
        F.row_number().over(w4).alias("event_seq"),
    )
)

result = (
    scored
    .select("id", "seq_no", "last_known_browser", "last_known_browser_2")
    .orderBy(F.col("id").desc())
)
