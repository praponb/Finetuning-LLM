from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("signup_date", "id")
w2 = Window.partitionBy("country").orderBy("age", "id")
w3 = Window.partitionBy("city").orderBy("credit_limit", "id").rowsBetween(-2, -1)

result = (
    df_customers
    .withColumn("next_age", F.lead("age", 1, 0).over(w))
    .withColumn("next_credit_limit", F.lead("credit_limit", 2).over(w2))
    .withColumn("credit_limit_avg_2_prior", F.avg("credit_limit").over(w3))
)
