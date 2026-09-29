from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("age").asc_nulls_first(), "id")

valid = (
    df_customers
    .filter((F.col("signup_date") > F.lit("2023-02-23").cast("date")) & (~F.col("city").eqNullSafe(F.lit("Chiang Mai"))))
    .withColumn("clean_city", F.trim("city"))
)

result = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)
