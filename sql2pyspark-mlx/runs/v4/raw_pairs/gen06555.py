from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("status_length").desc())
w2 = Window.partitionBy("customer_id").orderBy("status_length")

cleaned = (
    df_subscriptions
    .filter((F.col("end_date") >= F.lit("2023-07-07").cast("date")) & F.col("customer_id").isNotNull())
    .withColumn("status_length", F.length("status"))
)

result = (
    cleaned
    .withColumn("status_length_cume_dist", F.cume_dist().over(w))
    .withColumn("status_length_cume_dist_2", F.cume_dist().over(w2))
)
