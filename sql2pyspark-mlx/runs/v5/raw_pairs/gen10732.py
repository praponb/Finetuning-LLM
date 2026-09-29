from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("quarter_start").orderBy(F.col("mobile_rows").desc())

result = (
    df_sessions
    .groupBy(
        F.date_trunc("QUARTER", "started_at").alias("quarter_start"),
        F.date_trunc("YEAR", "started_at").alias("year_start"),
        "browser",
    )
    .agg(
        F.count_if(F.col("country") == "US").alias("us_events"),
        F.sort_array(F.collect_set("browser")).alias("browser_list"),
        F.count(F.when(F.col("device") == "mobile", 1)).alias("mobile_rows"),
    )
    .withColumn("mobile_rows_dense_rank", F.dense_rank().over(w))
    .orderBy(F.col("mobile_rows").asc_nulls_last())
)
