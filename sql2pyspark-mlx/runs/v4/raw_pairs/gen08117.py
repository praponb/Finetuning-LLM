from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser", "country").orderBy(F.col("started_at").desc())

result = (
    df_sessions
    .select(
        "id",
        "browser",
        "country",
        "started_at",
        F.percent_rank().over(w).alias("started_at_pct_rank"),
    )
)
