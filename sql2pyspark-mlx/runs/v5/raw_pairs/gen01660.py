from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("cost").desc())

result = (
    df_shipments
    .filter(F.col("cost") <= 87)
    .select(
        "id",
        "order_id",
        "cost",
        "shipped_date",
        F.dense_rank().over(w).alias("rn"),
    )
    .filter(F.col("rn") <= 1)
    .select("id", "order_id", "cost", "shipped_date")
)
