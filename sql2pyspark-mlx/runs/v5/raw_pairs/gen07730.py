from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating_rounded", "id")
w2 = Window.partitionBy("country")

base = (
    df_suppliers
    .filter(F.col("rating").isNotNull())
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.round("rating", 0).alias("rating_rounded"),
        F.col("rating").cast("int").alias("rating_int"),
    )
)

result = (
    base
    .select(
        "id",
        "country",
        "city",
        "rating",
        "rating_rounded",
        "rating_int",
        F.row_number().over(w).alias("row_seq"),
        F.round((F.col("rating") / F.sum("rating").over(w2)), 4).alias("rating_share"),
    )
)
