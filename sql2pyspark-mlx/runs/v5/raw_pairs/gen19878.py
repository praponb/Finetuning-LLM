from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("age").desc(), "id")

result = (
    df_customers
    .filter(F.col("credit_limit").isNotNull())
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") <= 1)
)
