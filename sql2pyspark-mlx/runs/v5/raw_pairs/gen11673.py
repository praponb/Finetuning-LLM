from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("credit_limit").desc())

ranked = (
    df_customers
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

result = (
    ranked
    .select("id", "email", "city", "signup_date", "is_active", "credit_limit", "age")
    .orderBy("id")
)
