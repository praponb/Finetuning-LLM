from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("signup_date", "id")

result = (
    df_customers
    .filter(F.col("credit_limit").isNotNull())
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") <= 2)
    .orderBy(F.col("name").desc())
)
