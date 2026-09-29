from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("signup_date").desc(), "id")

deduped = (
    df_customers
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    deduped
    .select(
        "id",
        "name",
        "email",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        "row_num",
    )
    .orderBy(F.col("id").desc())
    .limit(10)
)
