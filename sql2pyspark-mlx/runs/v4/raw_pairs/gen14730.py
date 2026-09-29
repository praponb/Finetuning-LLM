from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("cost").desc(), "id")

result = (
    df_shipments
    .filter(F.col("delivered_date").between("2024-07-28", "2025-10-01"))
    .select("id", "order_id", "cost", F.row_number().over(w).alias("rnk"))
    .filter(F.col("rnk") == 1)
    .orderBy(F.col("cost").desc())
)
