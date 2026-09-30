from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("rating", "id")
w2 = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country").orderBy("rating", "id")

scored = (
    df_suppliers
    .select(
        "*",
        F.ntile(3).over(w).alias("rating_bucket"),
        F.row_number().over(w).alias("seq_no"),
        F.last("rating", ignorenulls=True).over(w2).alias("last_known_rating"),
        F.lead("rating", 1, 0).over(w3).alias("next_rating"),
    )
)

result = (
    scored
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        "rating_bucket",
        "seq_no",
        "last_known_rating",
        "next_rating",
        F.when(F.col("country").rlike("^(TH|DE)$"), "OTHER_ZONE").when(F.col("country").rlike("^(US|SG)$"), "CORE").otherwise("GROUP_B").alias("country_zone"),
    )
    .orderBy(F.col("next_rating").desc_nulls_last(), F.col("country").desc())
)
