from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy("age", "id")

result = (
    df_customers
    .select(
        "id",
        "segment",
        "age",
        "is_active",
        F.row_number().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") <= 5)
    .orderBy(F.col("id").desc())
)
