from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city").orderBy(F.col("rating").desc_nulls_last(), "id")

base = (
    df_suppliers
    .filter(F.col("city").isNotNull() & F.col("supplier_name").like("S%"))
    .select(
        "*",
        F.coalesce("supplier_name", F.lit("Unknown")).alias("supplier_name_filled"),
    )
)

result = (
    base
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "supplier_name", "country", "city", "rating", "supplier_name_filled")
)
