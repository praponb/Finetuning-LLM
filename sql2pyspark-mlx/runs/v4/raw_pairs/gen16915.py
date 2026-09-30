from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("device").orderBy(F.col("started_at").desc(), "id")
w_grp = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("started_at", "id")
w5 = Window.partitionBy("browser").orderBy(F.col("started_at").desc(), "id")

metrics = (
    df_sessions
    .withColumn("last_known_browser", F.last("browser", ignorenulls=True).over(w))
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("last_known_country", F.last("country", ignorenulls=True).over(w_grp))
    .withColumn("row_seq_2", F.row_number().over(w4))
    .withColumn("seq_no", F.row_number().over(w5))
)

result = (
    metrics
    .groupBy(F.date_trunc("YEAR", "started_at").alias("year_start"))
    .agg(
        F.count_if(F.col("device") == "mobile").alias("mobile_events"),
        F.size(F.collect_set("browser")).alias("num_browsers"),
        F.count(F.when(F.col("device") == "desktop", 1)).alias("desktop_count"),
        F.count_if(F.col("browser") == "Safari").alias("safari_events"),
        F.count_if(F.col("country") == "SG").alias("sg_events"),
    )
    .orderBy(F.col("num_browsers").asc_nulls_first())
)
