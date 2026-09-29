from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("delivered_date").desc())

result = (
    df_shipments
    .filter(F.col("cost").between(72, 75))
    .select(
        "id",
        "carrier",
        "delivered_date",
        "status",
        F.rank().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") == 1)
    .select("id", "carrier", "delivered_date", "status")
)
