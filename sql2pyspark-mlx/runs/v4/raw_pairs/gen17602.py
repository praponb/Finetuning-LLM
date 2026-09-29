from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("cost_int", "id")
w_recent = Window.orderBy("shipped_date", "id")

filtered = (
    df_shipments
    .filter((F.col("shipped_date") > F.lit("2023-06-19").cast("date")) & (F.col("status") == "RETURNED"))
    .select("*", F.col("cost").cast("int").alias("cost_int"))
)

result = (
    filtered
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("prev_cost_int", F.lag("cost_int", 1).over(w_recent))
)
