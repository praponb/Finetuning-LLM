from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("city").orderBy(F.col("opened_date").desc(), "id")
w2 = Window.partitionBy("city").orderBy("opened_date", "id")

valid = (
    df_stores
    .filter((F.col("opened_date") >= F.lit("2023-02-17").cast("date")) & F.col("store_name").isNull())
    .select(
        "id",
        "store_name",
        "city",
        "opened_date",
        F.initcap(F.trim("store_name")).alias("store_name_title"),
        F.coalesce("region", F.lit("OTHER")).alias("region_filled"),
    )
)

result = (
    valid
    .withColumn("seq_no", F.row_number().over(w_hist))
    .withColumn("row_seq", F.row_number().over(w2))
)
