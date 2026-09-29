from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("browser").orderBy(F.col("started_at_date").desc(), "id")

base = (
    df_sessions
    .filter(F.col("device") != "desktop")
    .select(
        "id",
        "device",
        "browser",
        "started_at",
        F.to_date("started_at").alias("started_at_date"),
    )
)

result = (
    base
    .select(
        "id",
        "device",
        "browser",
        F.last("browser", ignorenulls=True).over(w).alias("last_known_browser"),
        F.last("browser", ignorenulls=True).over(w2).alias("last_known_browser_2"),
        F.row_number().over(w3).alias("event_seq"),
    )
)
