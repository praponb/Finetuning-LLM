from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("shipped_date", "id")
w2 = Window.partitionBy("carrier")

result = (
    df_shipments
    .select(
        "id",
        "carrier",
        "shipped_date",
        "cost",
        F.ntile(4).over(w).alias("bucket"),
        F.min("cost").over(w2).alias("carrier_min_cost"),
    )
)
