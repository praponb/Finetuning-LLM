from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id")
w_hist_2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_sessions
    .filter(F.col("device").isin(["mobile", "tablet"]))
    .select(
        "id",
        "started_at",
        F.last("country", ignorenulls=True).over(w_hist).alias("last_known_country"),
        F.row_number().over(w2).alias("event_seq"),
        F.first("browser", ignorenulls=True).over(w_hist_2).alias("first_known_browser"),
    )
)
