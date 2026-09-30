from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier")
w2 = Window.partitionBy("carrier", "status").orderBy(F.col("cost").desc(), "id")

result = (
    df_shipments
    .filter(F.year("shipped_date") == 2023)
    .select(
        "id",
        "carrier",
        "cost",
        "status",
        F.avg("cost").over(w).alias("carrier_avg_cost"),
        F.row_number().over(w2).alias("row_num"),
    )
)
