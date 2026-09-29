from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_sessions
    .filter(F.col("country").isin(["UK", "US", "SG"]))
    .select(
        "id",
        "device",
        "started_at",
        "country",
        F.row_number().over(w_hist).alias("event_seq"),
        F.first("country", ignorenulls=True).over(w2).alias("first_known_country"),
    )
)
