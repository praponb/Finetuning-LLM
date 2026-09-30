from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")
w2 = Window.partitionBy("city")

result = (
    df_customers
    .select(
        "id",
        "country",
        "age",
        "city",
        F.round((F.col("age") / F.sum("age").over(w)), 3).alias("age_share"),
        F.count("*").over(w2).alias("city_count"),
    )
)
