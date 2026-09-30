from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(-6, -1)
w2 = Window.partitionBy("country").orderBy(F.col("rating").asc_nulls_last())
w_running = Window.orderBy("rating", "id").rowsBetween(-5, -1)
w4 = Window.partitionBy("country", "city").orderBy(F.col("rating").desc(), "id")

enriched = (
    df_suppliers
    .select(
        "*",
        F.avg("rating").over(w).alias("rating_avg_6_prior"),
        F.cume_dist().over(w2).alias("rating_cume_dist"),
        F.avg("rating").over(w_running).alias("rating_avg_5_prior"),
        F.ntile(3).over(w4).alias("rating_bucket"),
    )
)

result = (
    enriched
    .select(
        "id",
        "supplier_name",
        "rating",
        "rating_cume_dist",
        "rating_avg_5_prior",
        "rating_bucket",
        F.when(F.col("supplier_name").rlike("^(Siam Parts Co|Pacific Wholesale)$"), "CORE").otherwise("GROUP_B").alias("supplier_name_zone"),
    )
    .orderBy(F.col("supplier_name_zone").asc_nulls_last(), "rating_bucket")
)
