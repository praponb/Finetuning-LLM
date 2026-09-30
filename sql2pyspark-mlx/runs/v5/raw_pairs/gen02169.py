from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("device", "customer_id").orderBy("started_at", "id")
w3 = Window.partitionBy("device", "customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_sessions
    .filter(F.hour("started_at").between(8, 14))
    .select(
        "id",
        "customer_id",
        "browser",
        F.first("country", ignorenulls=True).over(w).alias("first_known_country"),
        F.row_number().over(w2).alias("event_seq"),
        F.last("country", ignorenulls=True).over(w3).alias("last_known_country"),
    )
)
