from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy("signup_date", "id")

result = (
    df_customers
    .select("id", "segment", "signup_date", F.row_number().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 5)
    .orderBy(F.col("rnk").desc())
)
