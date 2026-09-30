from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")

result = (
    df_sessions
    .filter(~F.col("device").isin(["tablet", "desktop"]))
    .select(
        "id",
        "customer_id",
        "started_at",
        F.lag("started_at", 1).over(w).alias("prev_started_at"),
    )
)
