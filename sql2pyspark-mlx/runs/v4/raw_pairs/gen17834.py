from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("cost").desc(), "id")

result = (
    df_products
    .filter(F.col("stock_qty") == 357)
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") <= 3)
    .select("id", "supplier_id", "cost")
    .orderBy(F.col("id").desc())
)
