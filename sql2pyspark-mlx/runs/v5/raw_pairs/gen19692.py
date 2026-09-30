from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at")
w2 = Window.orderBy("started_at")

result = (
    df_sessions
    .filter(F.col("device") == "tablet")
    .select(
        "id",
        "country",
        "started_at",
        F.rank().over(w).alias("rnk"),
        F.rank().over(w2).alias("started_at_rank"),
    )
)
