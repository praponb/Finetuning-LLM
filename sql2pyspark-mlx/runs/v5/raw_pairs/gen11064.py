from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").desc(), "id")

filtered = (
    df_suppliers
    .filter(F.col("city").isin(["Singapore", "Tokyo"]))
    .select(
        "*",
        F.concat("city", F.lit(" - "), "supplier_name").alias("city_supplier_name"),
        F.concat("supplier_name", F.lit(" - "), "country").alias("supplier_name_country"),
    )
)

result = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)
