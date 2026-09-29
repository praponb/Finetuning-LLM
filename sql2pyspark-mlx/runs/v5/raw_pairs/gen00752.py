from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("cost").desc(), "id")

result = (
    df_shipments
    .filter(F.col("order_id").isin([10, 19, 36, 38]))
    .select("id", "cost", F.row_number().over(w).alias("row_num"))
    .orderBy(F.col("row_num").desc())
)
