from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")
w2 = Window.partitionBy("city").orderBy(F.col("age").desc())

result = (
    df_customers
    .select(
        "id",
        "country",
        "credit_limit",
        "city",
        "age",
        F.max("credit_limit").over(w).alias("country_max_credit_limit"),
        F.rank().over(w2).alias("rnk"),
    )
)
