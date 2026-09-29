from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("city").isin(["Tokyo", "Berlin"]))
    .withColumn("age_plus_credit_limit", (F.col("age") + F.col("credit_limit")))
    .orderBy("id")
)
