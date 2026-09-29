from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("price").desc(), "id")

result = (
    df_products
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") <= 3)
    .select("id", "supplier_id", "price", "launch_date")
    .orderBy(F.col("id").desc())
)
