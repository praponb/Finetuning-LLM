from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("status_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_shipments
    .filter(F.col("shipped_date") >= F.lit("2024-01-12").cast("date"))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        F.length("status").alias("status_length"),
    )
)

result = (
    cleaned
    .withColumn("cumulative_status_length", F.sum("status_length").over(w))
    .withColumn("running_status_length", F.sum("status_length").over(w2))
)
