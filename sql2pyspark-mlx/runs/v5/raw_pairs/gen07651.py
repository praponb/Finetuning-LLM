from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("shipped_date").desc())

result = (
    df_shipments
    .filter(F.col("status") != "IN_TRANSIT")
    .select(
        "id",
        "order_id",
        "shipped_date",
        "status",
        F.dense_rank().over(w).alias("pos"),
    )
    .filter(F.col("pos") <= 3)
)
