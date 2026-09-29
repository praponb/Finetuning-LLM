from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_duration_sec").desc())

result = (
    df_web_events
    .groupBy(
        F.when(F.col("event_type").isin(["search", "add_to_cart"]), "group_a").otherwise("other").alias("event_type_group"),
    )
    .agg(F.sum("duration_sec").alias("total_duration_sec"))
    .withColumn("rnk", F.dense_rank().over(w))
)
