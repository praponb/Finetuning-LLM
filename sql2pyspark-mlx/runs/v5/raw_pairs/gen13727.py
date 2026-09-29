from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").desc())

result = (
    df_suppliers
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
    .orderBy(F.col("id").desc())
)
