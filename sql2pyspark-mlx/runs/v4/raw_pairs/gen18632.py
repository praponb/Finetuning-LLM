from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city").orderBy(F.col("rating").desc(), "id")
w2 = Window.partitionBy("country").orderBy(F.col("rating").desc_nulls_first())
w3 = Window.partitionBy("country", "city").orderBy(F.col("rating").asc_nulls_first())

result = (
    df_suppliers
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("rating_dense_rank", F.dense_rank().over(w2))
    .withColumn("rating_percent_rank", F.percent_rank().over(w3))
)
