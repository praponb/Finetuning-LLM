from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_sessions
    .select(
        "id",
        "customer_id",
        "device",
        "country",
        F.last("browser", ignorenulls=True).over(w).alias("last_known_browser"),
        F.first("browser", ignorenulls=True).over(w_recent).alias("first_known_browser"),
        F.last("browser", ignorenulls=True).over(w3).alias("last_known_browser_2"),
    )
)
