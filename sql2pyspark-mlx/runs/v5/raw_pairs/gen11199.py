from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").desc(), "id")

result = (
    df_suppliers
    .select(
        "id",
        "country",
        "rating",
        "supplier_name",
        F.row_number().over(w).alias("pos"),
    )
    .filter(F.col("pos") == 1)
    .select("id", "country", "rating", "supplier_name")
)
