from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy("rating", "id")

result = (
    df_suppliers
    .filter(F.col("city") == "Tokyo")
    .select(
        "id",
        "city",
        "country",
        "rating",
        "supplier_name",
        F.first("supplier_name").over(w).alias("first_supplier_name"),
    )
)
