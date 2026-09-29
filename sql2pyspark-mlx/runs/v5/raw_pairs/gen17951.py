from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy(F.col("started_at").desc_nulls_last(), "id")

deduped = (
    df_sessions
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    deduped
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        "dedup_rank",
    )
)
