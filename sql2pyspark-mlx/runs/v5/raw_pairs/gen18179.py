from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser", "customer_id").orderBy(F.col("started_at").desc(), "id")
w_ordered = Window.partitionBy("country", "device").orderBy("started_at", "id")
w3 = Window.partitionBy("device", "customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_sessions
    .select(
        "id",
        "device",
        "browser",
        "started_at",
        "country",
        F.row_number().over(w).alias("row_seq"),
        F.row_number().over(w_ordered).alias("row_seq_2"),
        F.last("country", ignorenulls=True).over(w3).alias("last_known_country"),
        F.first("browser", ignorenulls=True).over(w4).alias("first_known_browser"),
    )
)
