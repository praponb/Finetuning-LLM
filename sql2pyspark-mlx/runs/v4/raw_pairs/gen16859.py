from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("country").orderBy("started_at", "id")
w3 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("customer_id", "device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_sessions
    .select(
        "*",
        F.last("browser", ignorenulls=True).over(w).alias("last_known_browser"),
        F.row_number().over(w_part).alias("event_seq"),
        F.last("country", ignorenulls=True).over(w3).alias("last_known_country"),
        F.first("country", ignorenulls=True).over(w4).alias("first_known_country"),
    )
)
