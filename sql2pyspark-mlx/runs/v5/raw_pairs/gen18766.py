from pyspark.sql import functions as F

result = (
    df_sessions
    .cube(F.date_trunc("quarter", "started_at").alias("quarter_start"), "device")
    .agg(
        F.sort_array(F.collect_set("device")).alias("device_list"),
        F.count(F.when(F.col("device") == "tablet", 1)).alias("tablet_count"),
        F.sort_array(F.collect_set("browser")).alias("browser_list"),
        F.size(F.collect_set("browser")).alias("num_browsers"),
    )
    .filter(F.col("num_browsers") > 1)
    .orderBy(F.col("tablet_count").desc_nulls_last())
)
