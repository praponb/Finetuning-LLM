from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy("signup_date", "id")
w2 = Window.orderBy("credit_limit", "id")
w3 = Window.partitionBy("country").orderBy(F.col("age").desc_nulls_last())
w4 = Window.partitionBy("country", "city").orderBy("age", "id")

scored = (
    df_customers
    .withColumn("next_age", F.lead("age", 1).over(w))
    .withColumn("next_age_2", F.lead("age", 1, 0).over(w2))
    .withColumn("age_rank", F.rank().over(w3))
    .withColumn("row_seq", F.row_number().over(w4))
)

result = (
    scored
    .select(
        "id",
        "name",
        "email",
        "country",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
    )
    .orderBy(F.col("id").desc_nulls_last(), "country")
    .limit(20)
)
