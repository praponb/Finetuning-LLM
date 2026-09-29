from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("shipped_date", "id")

result = (
    df_shipments
    .select(
        "id",
        "carrier",
        "shipped_date",
        "delivered_date",
        F.row_number().over(w).alias("pos"),
    )
    .filter(F.col("pos") <= 2)
    .orderBy("delivered_date", F.col("pos").desc())
)
