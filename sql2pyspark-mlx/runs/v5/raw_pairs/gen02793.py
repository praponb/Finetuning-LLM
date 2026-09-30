from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("cost").desc(), "id")

result = (
    df_shipments
    .filter(F.col("delivered_date") > "2024-12-01")
    .select("id", "carrier", "cost", "order_id", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") <= 3)
    .select("id", "carrier", "cost", "order_id")
)
