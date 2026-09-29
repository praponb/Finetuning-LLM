from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("rating").desc(), "id")
w2 = Window.partitionBy("city").orderBy(F.col("rating").desc(), "id")

with_windows = (
    df_suppliers
    .select(
        "*",
        F.ntile(3).over(w).alias("rating_bucket"),
        F.ntile(4).over(w2).alias("rating_bucket_2"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        "rating_bucket",
        "rating_bucket_2",
        F.when(F.col("supplier_name").rlike("^(Global Supply|Tokyo Traders)$"), "GROUP_A").otherwise("PRIORITY").alias("supplier_name_zone"),
    )
    .orderBy(F.col("id").desc(), F.col("rating").desc())
)
