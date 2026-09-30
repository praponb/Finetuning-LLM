from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "segment").orderBy(F.col("signup_date").desc_nulls_last(), "id")

cleaned = (
    df_customers
    .filter((~F.col("segment").eqNullSafe(F.lit("Corporate"))) & (F.col("signup_date") > F.to_date(F.lit("2023-08-26"))))
    .select("*", F.upper("country").alias("country_upper"))
)

result = (
    cleaned
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "city", "segment")
)
