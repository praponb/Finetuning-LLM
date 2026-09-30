from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("age", "id")

result = (
    df_customers
    .filter(F.col("credit_limit") > 14700)
    .select("id", "country", "age", F.ntile(4).over(w).alias("age_ntile"))
)
