from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("started_at").desc_nulls_first(), "id")

deduped = (
    df_sessions
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    deduped
    .select("id", "customer_id", "device", "browser", "started_at", "country")
    .orderBy("customer_id")
)
