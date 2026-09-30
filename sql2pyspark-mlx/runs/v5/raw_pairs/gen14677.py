from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "segment")
w2 = Window.orderBy("age", "id").rowsBetween(-2, Window.currentRow)

with_windows = (
    df_customers
    .select(
        "id",
        "name",
        "email",
        "country",
        "segment",
        "signup_date",
        "credit_limit",
        F.corr("credit_limit", "age").over(w).alias("corr_credit_limit_age"),
        F.avg("credit_limit").over(w2).alias("credit_limit_avg_2"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "name",
        "email",
        "country",
        "segment",
        "signup_date",
        "credit_limit",
        "credit_limit_avg_2",
        F.when(F.col("name").rlike("^(Grace Kim|Alice Wong)$"), "OTHER_ZONE").when(F.col("name").rlike("^(bob smith|  Carol Diaz)$"), "EXTENDED").otherwise("PRIORITY").alias("name_zone"),
    )
    .orderBy("id", F.col("name_zone").desc())
)
