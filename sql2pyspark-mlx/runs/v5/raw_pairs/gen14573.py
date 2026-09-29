from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")
w2 = Window.partitionBy("city")
w3 = Window.partitionBy("segment").orderBy(F.col("credit_limit").desc(), "id")

result = (
    df_customers
    .select(
        "id",
        "name",
        "email",
        "country",
        "signup_date",
        "credit_limit",
        F.round((F.col("credit_limit") / F.sum("credit_limit").over(w)), 4).alias("credit_limit_share"),
        F.round((F.col("age") / F.sum("age").over(w2)), 4).alias("age_share"),
        F.ntile(5).over(w3).alias("credit_limit_bucket"),
    )
)
