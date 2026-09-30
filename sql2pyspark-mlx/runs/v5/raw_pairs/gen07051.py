from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment", "city").orderBy(F.col("signup_date").desc(), "id")

result = (
    df_customers
    .select(
        "id",
        "segment",
        "city",
        "signup_date",
        F.ntile(10).over(w).alias("quartile"),
    )
    .orderBy(F.col("id").desc())
)
