from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("signup_date").desc(), "id")

result = (
    df_customers
    .select(
        "id",
        "segment",
        "signup_date",
        "country",
        F.row_number().over(w).alias("rn"),
    )
    .filter(F.col("rn") == 1)
    .select("id", "segment", "signup_date", "country")
    .orderBy(F.col("id").desc())
)
