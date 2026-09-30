from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").desc_nulls_last(), "id")

deduped = (
    df_suppliers
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "latest_rank",
        F.when(F.col("country").rlike("^(DE|TH)$"), "EXTENDED").otherwise("OTHER_ZONE").alias("country_zone"),
    )
    .orderBy(F.col("city").desc_nulls_first(), F.col("country").desc())
)
