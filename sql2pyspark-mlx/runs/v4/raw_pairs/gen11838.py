from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "browser").orderBy(F.col("started_at").desc(), "id")

ranked = (
    df_sessions
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    ranked
    .groupBy("customer_id", "browser")
    .agg(
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.min("started_at").alias("first_started_at"),
        F.countDistinct("id").alias("distinct_ids"),
        F.size(F.collect_set("device")).alias("num_devices"),
    )
    .orderBy(F.col("browser").asc_nulls_first())
)
