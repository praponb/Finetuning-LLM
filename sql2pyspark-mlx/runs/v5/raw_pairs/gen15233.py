from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.partitionBy("city", "country").orderBy("signup_date", "id")

result = (
    df_customers
    .filter(F.col("signup_date") > "2024-06-01")
    .select(
        "id",
        "city",
        "age",
        "country",
        "signup_date",
        F.avg("age").over(w).alias("city_avg_age"),
        F.lead("age", 1).over(w2).alias("next_age"),
    )
)
