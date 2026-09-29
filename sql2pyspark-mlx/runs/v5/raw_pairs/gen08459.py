from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id")

result = (
    df_suppliers
    .select(
        "id",
        "city",
        "rating",
        "supplier_name",
        F.row_number().over(w).alias("pos"),
    )
    .filter(F.col("pos") <= 3)
    .select("id", "city", "rating", "supplier_name")
    .orderBy(F.col("rating").desc())
)
