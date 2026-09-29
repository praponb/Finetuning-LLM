from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("city").orderBy("signup_date", "id")

result = (
    df_customers
    .withColumn("prev_credit_limit", F.lag("credit_limit").over(w_part))
    .withColumn("prev_age", F.lag("age").over(w_part))
    .withColumn("row_seq", F.row_number().over(w_part))
)
