from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").asc_nulls_last(), "id")

ranked = (
    df_suppliers
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    ranked
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        "latest_rank",
        F.when(F.col("supplier_name").rlike("^(Pacific Wholesale|Tokyo Traders)$"), "OTHER_ZONE").when(F.col("supplier_name").rlike("^(Siam Parts Co|Global Supply)$"), "CORE").otherwise("PRIORITY").alias("supplier_name_zone"),
    )
)
