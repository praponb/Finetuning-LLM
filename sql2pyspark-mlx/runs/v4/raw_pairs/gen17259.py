from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("units").desc(), "id")
w2 = Window.orderBy(F.col("sale_date").desc(), "id")

filtered = (
    df_store_sales
    .filter(F.col("sale_date") >= F.to_date(F.lit("2023-04-28")))
    .select("*", F.date_format("sale_date", "MMM yyyy").alias("sale_date_label"))
)

result = (
    filtered
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("row_seq", F.row_number().over(w2))
)
