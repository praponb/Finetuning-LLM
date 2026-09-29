from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy(F.col("rating_whole").desc_nulls_last(), "id")

valid = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        F.upper(F.trim("supplier_name")).alias("supplier_name_normalized"),
        F.ceil("rating").alias("rating_whole"),
    )
)

result = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)
