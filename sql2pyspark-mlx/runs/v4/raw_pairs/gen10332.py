from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("quantity").desc(), "id")

result = (
    df_inventory
    .filter(F.col("quantity") > 99)
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") <= 2)
)
