from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")

result = (
    df_customers
    .filter(F.col("name").like("Pi%"))
    .select(
        "id",
        "city",
        "credit_limit",
        F.sum("credit_limit").over(w).alias("city_sum_credit_limit"),
    )
)
