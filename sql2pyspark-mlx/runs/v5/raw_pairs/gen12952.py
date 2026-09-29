from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy(F.col("started_at").desc(), "id")

result = (
    df_sessions
    .select(
        "id",
        "device",
        "started_at",
        "customer_id",
        F.row_number().over(w).alias("rnk"),
    )
    .filter(F.col("rnk") <= 3)
    .select("id", "device", "started_at", "customer_id")
    .orderBy("id", "started_at")
)
