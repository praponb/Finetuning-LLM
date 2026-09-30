from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")
w2 = Window.partitionBy("store_id").orderBy(F.col("units").desc(), "id")

result = (
    df_store_sales
    .filter(F.col("units") != 38)
    .select(
        "id",
        "store_id",
        "revenue",
        "units",
        F.round((F.col("revenue") / F.sum("revenue").over(w)), 3).alias("revenue_share"),
        F.row_number().over(w2).alias("rn"),
    )
)
