from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("started_at").desc(), "id")
w2 = Window.partitionBy("country").orderBy(F.col("started_at").desc(), "id")
w3 = Window.partitionBy("customer_id", "device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_sessions
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("seq_no", F.row_number().over(w2))
    .withColumn("last_known_country", F.last("country", ignorenulls=True).over(w3))
)

result = (
    metrics
    .cube("country")
    .agg(
        F.round(F.sum("seq_no") / F.nullif(F.count(F.when(F.col("country") == "DE", 1)), F.lit(0)), 2).alias("seq_no_per_de"),
        F.sum(F.when(F.col("country") == "TH", F.col("row_seq")).otherwise(0)).alias("th_row_seq"),
        F.countDistinct("device").alias("distinct_devices"),
        F.grouping("country").alias("gid"),
    )
    .orderBy(F.col("distinct_devices").desc())
)
