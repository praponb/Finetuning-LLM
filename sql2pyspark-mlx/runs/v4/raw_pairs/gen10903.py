from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("age").desc_nulls_first(), "id")

result = (
    df_customers
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)
