from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("price", "id")

deduped = (
    df_products
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    deduped
    .select(
        "id",
        "brand",
        "launch_date",
        "tags",
        "row_num",
        F.when(F.col("brand").rlike("^(Globex|Umbrella)$"), "PRIORITY").when(F.col("brand").rlike("^(Acme|Initech)$"), "EXTENDED").otherwise("GROUP_B").alias("brand_zone"),
    )
    .orderBy(F.col("row_num").desc())
)
