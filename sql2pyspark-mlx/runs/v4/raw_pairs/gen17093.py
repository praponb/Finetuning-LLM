from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.partitionBy("country", "customer_id").orderBy(F.col("started_at").desc(), "id")
w3 = Window.partitionBy("customer_id").orderBy("started_at", "id")
w4 = Window.partitionBy("country", "browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_sessions
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("seq_no", F.row_number().over(w3))
    .withColumn("first_known_browser", F.first("browser", ignorenulls=True).over(w4))
    .withColumn("first_known_browser_2", F.first("browser", ignorenulls=True).over(w5))
)

result = (
    metrics
    .cube(F.date_trunc("year", "started_at").alias("year_start"), "browser")
    .agg(
        F.round(F.sum("event_seq") / F.nullif(F.count(F.when(F.col("browser") == "Firefox", 1)), F.lit(0)), 2).alias("event_seq_per_firefox"),
        F.count(F.when(F.col("browser") == "Firefox", 1)).alias("firefox_count"),
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("device") == "desktop", 1)).alias("desktop_rows"),
        F.count(F.when(F.col("country") == "UK", 1)).alias("uk_count"),
        F.min("event_seq").alias("min_event_seq"),
    )
)
