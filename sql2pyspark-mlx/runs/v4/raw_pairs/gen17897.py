from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id", "browser").orderBy("started_at", "id")

scored = (
    df_sessions
    .select(
        "*",
        F.last("browser", ignorenulls=True).over(w).alias("last_known_browser"),
        F.last("country", ignorenulls=True).over(w2).alias("last_known_country"),
        F.row_number().over(w3).alias("seq_no"),
    )
)

result = (
    scored
    .select("id", "customer_id", "device", "browser", "started_at", "country", "seq_no")
    .orderBy(F.col("country").desc_nulls_first(), "seq_no")
)
