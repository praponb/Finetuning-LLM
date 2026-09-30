from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("credit_limit", "id")

result = (
    df_customers
    .filter(F.col("city") == "London")
    .select("id", "city", "credit_limit", F.row_number().over(w).alias("row_num"))
)
