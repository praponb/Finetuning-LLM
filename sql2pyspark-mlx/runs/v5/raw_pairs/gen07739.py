from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("credit_limit").desc(), "id")

result = (
    df_customers
    .filter(F.col("city") == "New York")
    .select("id", "city", "credit_limit", "name", F.row_number().over(w).alias("pos"))
    .filter(F.col("pos") <= 3)
    .orderBy(F.col("pos").desc())
)
