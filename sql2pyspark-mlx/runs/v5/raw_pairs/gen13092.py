from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("shipped_date").desc())

result = (
    df_shipments
    .select(
        "id",
        "carrier",
        "shipped_date",
        "order_id",
        F.dense_rank().over(w).alias("pos"),
    )
    .filter(F.col("pos") <= 3)
    .select("id", "carrier", "shipped_date", "order_id")
    .orderBy(F.col("id").desc())
)
