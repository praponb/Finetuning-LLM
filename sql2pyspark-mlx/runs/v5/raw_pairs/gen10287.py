from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(-2, Window.currentRow)

scored = (
    df_suppliers
    .select(
        "id",
        "supplier_name",
        "rating",
        F.max("rating").over(w).alias("last3_max_rating"),
        F.min("rating").over(w).alias("last3_min_rating"),
        F.count("rating").over(w).alias("last3_count_rating"),
    )
)

result = (
    scored
    .select(
        "id",
        "supplier_name",
        "rating",
        "last3_min_rating",
        F.when(F.col("supplier_name").rlike("^(Pacific Wholesale|Lion City Imports)$"), "GROUP_B").when(F.col("supplier_name").rlike("^(Siam Parts Co|Global Supply)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("supplier_name_zone"),
    )
    .orderBy(F.col("last3_min_rating").desc())
)
