from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("cost").desc())

result = (
    df_shipments
    .filter(F.col("shipped_date").between("2023-08-04", "2025-06-01"))
    .select("id", "cost", F.rank().over(w).alias("cost_rank"))
    .orderBy("cost_rank")
)
