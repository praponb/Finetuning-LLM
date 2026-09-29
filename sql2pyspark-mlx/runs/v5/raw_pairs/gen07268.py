from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("units").desc(), "id")
w2 = Window.partitionBy("store_id").orderBy(F.col("sale_date").desc(), "id")

valid = (
    df_store_sales
    .filter(F.col("revenue") < 3900)
    .withColumn("sale_date_minus_23d", F.date_sub("sale_date", 23))
)

result = (
    valid
    .select(
        "id",
        "store_id",
        "units",
        F.row_number().over(w).alias("event_seq"),
        F.row_number().over(w2).alias("row_seq"),
    )
)
