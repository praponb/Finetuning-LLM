from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy("credit_limit", "id")

result = (
    df_customers
    .select(
        "id",
        "segment",
        "credit_limit",
        "country",
        F.row_number().over(w).alias("rn"),
    )
    .filter(F.col("rn") == 1)
)
