from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy("signup_date", "id")

result = (
    df_customers
    .filter(~F.col("country").isin(["TH", "UK"]))
    .select("id", "segment", "signup_date", F.row_number().over(w).alias("row_num"))
)
