from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("location_group")

result = (
    df_departments
    .groupBy(
        F.when(F.col("location").isin(["Singapore", "New York"]), "core").otherwise("other").alias("location_group"),
    )
    .agg(
        F.sum("budget").alias("total_budget"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cumulative_budget", F.sum("total_budget").over(w))
    .orderBy("location_group", F.col("cnt").desc())
)
