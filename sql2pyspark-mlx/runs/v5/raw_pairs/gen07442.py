from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device").orderBy(F.col("started_at").desc(), "id")
w4 = Window.partitionBy("device", "country").orderBy(F.col("started_at").desc(), "id")

result = (
    df_sessions
    .select(
        "id",
        "browser",
        F.row_number().over(w).alias("seq_no"),
        F.last("browser", ignorenulls=True).over(w2).alias("last_known_browser"),
        F.row_number().over(w3).alias("event_seq"),
        F.row_number().over(w4).alias("seq_no_2"),
    )
)
