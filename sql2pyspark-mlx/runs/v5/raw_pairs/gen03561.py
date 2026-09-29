from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy(F.col("rating").desc_nulls_last(), "id")

deduped = (
    df_suppliers
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    deduped
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.when(F.col("supplier_name").rlike("^(Lion City Imports|Global Supply)$"), "PRIORITY").otherwise("OTHER_ZONE").alias("supplier_name_zone"),
    )
)
