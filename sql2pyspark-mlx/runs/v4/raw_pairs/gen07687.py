from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").desc())

result = (
    df_suppliers
    .filter(F.col("city") == "Singapore")
    .select(
        "id",
        "city",
        "rating",
        "supplier_name",
        F.dense_rank().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") == 1)
    .select("id", "city", "rating", "supplier_name")
)
