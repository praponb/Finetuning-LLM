from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("signup_date", "id")

result = (
    df_customers
    .select("id", "signup_date", "age", F.lag("age", 1).over(w).alias("prev_age"))
)
