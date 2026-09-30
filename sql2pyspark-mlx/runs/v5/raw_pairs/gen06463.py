from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("started_at").desc())
w2 = Window.partitionBy("device", "browser").orderBy("started_at")

result = (
    df_sessions
    .filter(F.col("country") != "SG")
    .select(
        "id",
        "customer_id",
        "started_at",
        "device",
        "browser",
        F.rank().over(w).alias("rnk"),
        F.rank().over(w2).alias("started_at_rank"),
    )
)
