from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy(F.col("started_at").desc(), "id")

result = (
    df_sessions
    .select(
        "id",
        "browser",
        "started_at",
        "customer_id",
        F.row_number().over(w).alias("rn"),
    )
    .filter(F.col("rn") == 1)
    .select("id", "browser", "started_at", "customer_id")
    .orderBy(F.col("browser").desc(), "started_at")
)
