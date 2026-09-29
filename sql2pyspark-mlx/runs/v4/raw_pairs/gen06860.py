from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")
w2 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_sessions
    .filter((F.col("started_at") > F.lit("2023-08-18").cast("date")) & F.col("country").isin(["US", "SG"]))
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.when(F.col("country") == "TH", 1).otherwise(0).alias("is_th"),
        F.upper("country").alias("country_upper"),
    )
)

with_gaps = (
    filtered
    .withColumn("new_group_flag", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("burst_id", F.sum("new_group_flag").over(w2))

result = (
    sessionized
    .groupBy("customer_id")
    .agg(
        F.sum("new_group_flag").alias("total_new_group_flag"),
        F.countDistinct("device").alias("distinct_devices"),
        F.count(F.when(F.col("browser") == "Edge", 1)).alias("edge_count"),
    )
    .orderBy(F.col("edge_count").desc())
)
