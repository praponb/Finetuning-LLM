from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("cost").desc())

result = (
    df_products
    .withColumn("pos", F.rank().over(w))
    .filter(F.col("pos") == 1)
    .select("id", "supplier_id", "cost", "launch_date")
    .orderBy(F.col("id").desc())
)
