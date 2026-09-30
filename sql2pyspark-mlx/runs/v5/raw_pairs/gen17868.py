from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("browser").orderBy("started_at", "id")
w_part = Window.partitionBy("customer_id").orderBy(F.col("started_at").desc(), "id")

result = (
    df_sessions
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        "country",
        F.first("browser", ignorenulls=True).over(w).alias("first_known_browser"),
        F.row_number().over(w_ordered).alias("seq_no"),
        F.row_number().over(w_part).alias("row_seq"),
    )
)
