from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("started_at").desc(), "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_sessions
    .filter(F.hour("started_at").between(7, 14))
    .select(
        "id",
        "customer_id",
        F.row_number().over(w).alias("event_seq"),
        F.last("browser", ignorenulls=True).over(w2).alias("last_known_browser"),
        F.first("country", ignorenulls=True).over(w3).alias("first_known_country"),
        F.first("country", ignorenulls=True).over(w3).alias("first_known_country_2"),
    )
)
