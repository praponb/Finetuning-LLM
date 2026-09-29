from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").desc())

result = (
    df_suppliers
    .withColumn("pos", F.rank().over(w))
    .filter(F.col("pos") <= 5)
    .orderBy(F.col("pos").desc())
)
