from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy(F.col("sale_date").desc(), "id")

scored = (
    df_store_sales
    .withColumn("running_units", F.sum("units").over(w))
    .withColumn("event_seq", F.row_number().over(w2))
)

result = (
    scored
    .groupBy("product_id")
    .agg(
        F.min("running_units").alias("min_running_units"),
        F.round(F.avg("event_seq"), 2).alias("avg_event_seq"),
        F.bit_or("units").alias("units_bit_or"),
    )
)
