from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")

result = (
    df_sessions
    .filter(F.hour("started_at").between(10, 17))
    .select(
        "id",
        "customer_id",
        "started_at",
        "device",
        F.row_number().over(w).alias("rn"),
    )
    .filter(F.col("rn") <= 3)
    .select("id", "customer_id", "started_at", "device")
    .orderBy("device")
)
