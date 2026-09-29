from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device", "country").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id")
w3 = Window.partitionBy("customer_id", "browser").orderBy(F.col("started_at").desc(), "id")
w_grp = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_sessions
    .select(
        "id",
        "customer_id",
        "country",
        F.row_number().over(w).alias("seq_no"),
        F.row_number().over(w2).alias("row_seq"),
        F.row_number().over(w3).alias("event_seq"),
        F.first("browser", ignorenulls=True).over(w_grp).alias("first_known_browser"),
        F.last("browser", ignorenulls=True).over(w5).alias("last_known_browser"),
    )
)
