from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("age").desc(), "id")
w2 = Window.partitionBy("segment")

result = (
    df_customers
    .select(
        "id",
        "name",
        "city",
        "signup_date",
        "is_active",
        "age",
        F.row_number().over(w).alias("event_seq"),
        F.round((F.col("age") / F.sum("age").over(w2)), 4).alias("age_share"),
    )
)
