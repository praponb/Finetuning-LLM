from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("signup_date").desc(), "id")

result = (
    df_customers
    .filter(F.col("country").isin(["DE", "US", "TH"]))
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") <= 2)
    .orderBy("country")
)
