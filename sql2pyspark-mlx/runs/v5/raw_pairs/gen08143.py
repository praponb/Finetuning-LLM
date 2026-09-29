from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("started_at").desc(), "id")
w_hist = Window.partitionBy("device").orderBy("started_at", "id")
w3 = Window.partitionBy("country").orderBy("started_at", "id")

result = (
    df_sessions
    .select(
        "id",
        "started_at",
        F.row_number().over(w).alias("row_seq"),
        F.row_number().over(w_hist).alias("seq_no"),
        F.row_number().over(w3).alias("row_seq_2"),
    )
)
