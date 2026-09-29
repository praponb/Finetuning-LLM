from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("credit_limit").desc(), "id")

result = (
    df_customers
    .select(
        "id",
        "segment",
        "credit_limit",
        "email",
        F.row_number().over(w).alias("rnk"),
    )
    .filter(F.col("rnk") == 1)
    .select("id", "segment", "credit_limit", "email")
    .orderBy(F.col("credit_limit").desc())
)
