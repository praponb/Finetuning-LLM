from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("started_at").desc(), "id")

result = (
    df_sessions
    .filter(F.col("device").isin(["tablet", "desktop"]))
    .select(
        "id",
        "customer_id",
        "started_at",
        "country",
        F.row_number().over(w).alias("rnk"),
    )
    .filter(F.col("rnk") <= 3)
    .select("id", "customer_id", "started_at", "country")
)
