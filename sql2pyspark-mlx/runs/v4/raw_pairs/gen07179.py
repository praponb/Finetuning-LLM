from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("brand").orderBy("cost", "id")

metrics = (
    df_products
    .withColumn("running_stock_qty", F.sum("stock_qty").over(w))
    .withColumn("seq_no", F.row_number().over(w2))
)

result = (
    metrics
    .select("id", "product_name", "brand", "cost", "stock_qty", "launch_date", "seq_no")
    .orderBy(F.col("id").asc_nulls_first(), "seq_no")
)
