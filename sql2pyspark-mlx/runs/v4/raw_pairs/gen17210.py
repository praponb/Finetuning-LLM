from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("signup_date").desc(), "id")

result = (
    df_customers
    .select("id", "country", "signup_date", "name", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") <= 3)
    .orderBy(F.col("country").desc())
)
