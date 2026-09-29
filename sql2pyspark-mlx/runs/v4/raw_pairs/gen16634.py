from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("shipped_date", "id")

result = (
    df_shipments
    .filter(F.col("status") != "IN_TRANSIT")
    .select("id", "shipped_date", "cost", F.sum("cost").over(w).alias("running_cost"))
    .orderBy(F.col("running_cost").desc())
)
