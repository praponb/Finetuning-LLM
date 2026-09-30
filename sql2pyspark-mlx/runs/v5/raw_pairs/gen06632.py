from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device").orderBy(F.col("started_at").desc(), "id")
w4 = Window.partitionBy("customer_id").orderBy(F.col("started_at").desc(), "id")

result = (
    df_sessions
    .filter(F.to_date("started_at") < "2023-10-10")
    .select(
        "id",
        "browser",
        "started_at",
        F.row_number().over(w).alias("event_seq"),
        F.last("browser", ignorenulls=True).over(w2).alias("last_known_browser"),
        F.row_number().over(w3).alias("seq_no"),
        F.row_number().over(w4).alias("event_seq_2"),
    )
)
