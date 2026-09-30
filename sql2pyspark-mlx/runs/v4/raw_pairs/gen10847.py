from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("age").desc_nulls_first())
w2 = Window.partitionBy("segment").orderBy("age", "id")

metrics = (
    df_customers
    .select(
        "*",
        F.cume_dist().over(w).alias("age_cume_dist"),
        F.lead("age", 1).over(w2).alias("next_age"),
    )
)

result = (
    metrics
    .select(
        "id",
        "email",
        "country",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        "next_age",
    )
    .orderBy("id")
)
