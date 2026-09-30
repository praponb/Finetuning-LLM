from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region", "city").orderBy(F.col("sqft").desc(), "id")
w2 = Window.partitionBy("city", "region")
w3 = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("region").orderBy("sqft", "id")
w5 = Window.partitionBy("region", "city").orderBy("sqft", "id")

enriched = (
    df_stores
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("sqft_share", F.round((F.col("sqft") / F.sum("sqft").over(w2)), 4))
    .withColumn("cumulative_sqft", F.sum("sqft").over(w3))
    .withColumn("row_seq_2", F.row_number().over(w_recent))
    .withColumn("sqft_bucket", F.ntile(4).over(w5))
)

result = (
    enriched
    .select("id", "city", "region", "sqft", "row_seq", "cumulative_sqft", "row_seq_2")
    .orderBy(F.col("city").asc_nulls_last(), "id")
)
