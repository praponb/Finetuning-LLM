from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")
w2 = Window.partitionBy("city").orderBy("rating", "id")

result = (
    df_suppliers
    .select(
        "id",
        "country",
        "city",
        "rating",
        "supplier_name",
        F.count("*").over(w).alias("country_count"),
        F.first("supplier_name").over(w2).alias("first_supplier_name"),
    )
    .orderBy(F.col("supplier_name").desc())
)
