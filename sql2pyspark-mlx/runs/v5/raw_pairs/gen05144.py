from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("end_date").desc_nulls_first(), "id")

filtered = (
    df_subscriptions
    .filter((F.col("start_date") >= F.lit("2023-02-27").cast("date")) & F.col("customer_id").isNotNull())
    .select("*", F.trim("plan").alias("clean_plan"))
)

result = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)
