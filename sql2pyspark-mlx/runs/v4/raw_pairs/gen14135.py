from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")
w2 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_sessions
    .withColumn("new_group_flag", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_no", F.sum("new_group_flag").over(w2))

result = (
    islands
    .groupBy(
        F.date_trunc("QUARTER", "started_at").alias("quarter_start"),
        F.date_trunc("YEAR", "started_at").alias("year_start"),
        "browser",
    )
    .agg(
        F.count(F.when(F.col("country") == "UK", 1)).alias("uk_rows"),
        F.max("started_at").alias("last_started_at"),
        F.max("started_at").alias("last_started_at_2"),
        F.count("*").alias("cnt"),
        F.round(F.sum("new_group_flag") / F.nullif(F.count(F.when(F.col("browser") == "Firefox", 1)), F.lit(0)), 2).alias("new_group_flag_per_firefox"),
    )
    .filter(F.col("uk_rows") < 1)
    .orderBy(F.col("new_group_flag_per_firefox").desc_nulls_last())
)
