from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id", "country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_sessions
    .filter(F.col("device") != "mobile")
    .withColumn("started_at_period", F.date_trunc("month", "started_at"))
)

result = (
    filtered
    .select(
        "id",
        "country",
        "started_at_period",
        F.last("country", ignorenulls=True).over(w).alias("last_known_country"),
        F.first("browser", ignorenulls=True).over(w2).alias("first_known_browser"),
    )
)
