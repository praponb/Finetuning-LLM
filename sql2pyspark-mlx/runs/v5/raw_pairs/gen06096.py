from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("started_at", "id")
w2 = Window.partitionBy("country").orderBy("started_at", "id")
w3 = Window.partitionBy("device", "country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_sessions
    .select(
        "id",
        "device",
        F.row_number().over(w).alias("row_seq"),
        F.row_number().over(w2).alias("seq_no"),
        F.row_number().over(w2).alias("seq_no_2"),
        F.first("browser", ignorenulls=True).over(w3).alias("first_known_browser"),
    )
)
