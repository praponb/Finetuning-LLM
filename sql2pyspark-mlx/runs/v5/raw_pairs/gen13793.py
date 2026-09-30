from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")

result = (
    df_customers
    .withColumn("avg_age_by_country", F.avg("age").over(w))
    .filter(F.col("age") < F.col("avg_age_by_country"))
    .select("id", "country", "age", "is_active")
    .orderBy("age")
)
