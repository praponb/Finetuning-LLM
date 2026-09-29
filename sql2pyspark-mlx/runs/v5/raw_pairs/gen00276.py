from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("signup_date").desc())

result = (
    df_customers
    .filter(F.col("email").like("%@yahoo.com"))
    .withColumn("rnk", F.rank().over(w))
    .filter(F.col("rnk") <= 2)
    .select("id", "city", "signup_date")
)
