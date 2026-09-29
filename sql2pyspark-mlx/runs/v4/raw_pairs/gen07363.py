from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("num_countries").desc())

with_dims = (
    df_sessions
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "left")
    .select("e.*", F.col("c.name"))
)

result = (
    with_dims
    .groupBy(
        "customer_id",
        F.date_trunc("QUARTER", "started_at").alias("quarter_start"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.size(F.collect_set("country")).alias("num_countries"),
        F.size(F.collect_set("device")).alias("num_devices"),
    )
    .withColumn("num_countries_percent_rank", F.percent_rank().over(w))
    .orderBy(F.col("cnt").desc())
)
