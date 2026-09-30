from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("started_at_hour").desc(), "id")

base = (
    df_sessions
    .filter((F.col("started_at") > F.to_date(F.lit("2023-09-05"))) & F.col("country").isNotNull())
    .withColumn("started_at_hour", F.hour("started_at"))
)

result = (
    base
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)
