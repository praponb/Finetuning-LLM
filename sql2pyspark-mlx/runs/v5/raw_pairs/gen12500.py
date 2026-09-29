from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "device").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id")

result = (
    df_sessions
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        F.lead("started_at", 1).over(w).alias("next_started_at"),
        F.lag("started_at", 1).over(w2).alias("prev_started_at"),
    )
)
