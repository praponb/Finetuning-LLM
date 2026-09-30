from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("device", "browser").orderBy(F.col("started_at").desc(), "id")
w3 = Window.partitionBy("customer_id").orderBy(F.col("started_at").desc(), "id")
w4 = Window.orderBy("started_at", "id")
w5 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_sessions
    .select(
        "id",
        "browser",
        "country",
        F.last("browser", ignorenulls=True).over(w).alias("last_known_browser"),
        F.row_number().over(w2).alias("seq_no"),
        F.row_number().over(w3).alias("event_seq"),
        F.row_number().over(w4).alias("event_seq_2"),
        F.last("browser", ignorenulls=True).over(w5).alias("last_known_browser_2"),
    )
)

result = with_windows.select("id", "last_known_browser", "last_known_browser_2")
