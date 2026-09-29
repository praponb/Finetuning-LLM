from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("credit_limit").desc_nulls_first())

filtered = (
    df_customers
    .filter((F.col("signup_date") >= F.lit("2023-03-21").cast("date")) & (~F.col("city").eqNullSafe(F.lit("Singapore"))))
    .select("*", F.date_format("signup_date", "MMM yyyy").alias("signup_date_label"))
)

result = (
    filtered
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "email", "city", "segment", "credit_limit", "age")
)
