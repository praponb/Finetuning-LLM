from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")
w2 = Window.orderBy(F.col("started_at").desc(), "id")

cleaned = (
    df_sessions
    .filter(F.col("started_at") > F.lit("2023-06-15").cast("date"))
    .select(
        "id",
        "device",
        "browser",
        "started_at",
        "country",
        F.initcap(F.trim("browser")).alias("browser_title"),
    )
)

result = (
    cleaned
    .filter(F.col("device").isin(["tablet", "mobile"]))
    .select(
        "*",
        F.row_number().over(w).alias("seq_no"),
        F.row_number().over(w2).alias("seq_no_2"),
    )
)
