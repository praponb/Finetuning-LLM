from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("country", "customer_id").orderBy(F.col("started_at").desc(), "id")
w_grp = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("customer_id", "device").orderBy("started_at", "id")

result = (
    df_sessions
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "country",
        F.first("country", ignorenulls=True).over(w).alias("first_known_country"),
        F.row_number().over(w_hist).alias("seq_no"),
        F.last("browser", ignorenulls=True).over(w_grp).alias("last_known_browser"),
        F.row_number().over(w4).alias("event_seq"),
        F.last("country", ignorenulls=True).over(w_grp).alias("last_known_country"),
    )
)
