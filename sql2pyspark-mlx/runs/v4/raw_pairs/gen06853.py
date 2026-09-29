from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy("signup_date", "id")

result = (
    df_customers
    .filter(F.col("is_active"))
    .select("id", "segment", "signup_date", F.ntile(10).over(w).alias("bucket"))
)
