from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id", "browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_sessions
    .withColumn("last_known_country", F.last("country", ignorenulls=True).over(w))
    .withColumn("last_known_browser", F.last("browser", ignorenulls=True).over(w2))
)

result = (
    with_windows
    .rollup(F.date_trunc("QUARTER", "started_at").alias("quarter_start"))
    .agg(
        F.count(F.when(F.col("browser") == "Chrome", 1)).alias("chrome_rows"),
        F.count_if(F.col("country") == "JP").alias("jp_events"),
        F.min("started_at").alias("first_started_at"),
    )
    .orderBy(F.col("quarter_start").desc_nulls_first())
)
