from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("shipped_date")

result = (
    df_shipments
    .select(
        "id",
        "status",
        "shipped_date",
        "delivered_date",
        F.rank().over(w).alias("rn"),
    )
    .filter(F.col("rn") == 1)
    .select("id", "status", "shipped_date", "delivered_date")
)
