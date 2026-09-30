from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("credit_limit", "id")

result = (
    df_customers
    .select("id", "credit_limit", F.row_number().over(w).alias("credit_limit_row"))
)
