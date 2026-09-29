from pyspark.sql import functions as F

not_flagged = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.month(F.col("x.launch_date")).isin([3, 10, 11])), "left_semi")
)

result = (
    not_flagged
    .select(
        "id",
        "country",
        "city",
        "rating",
        F.when(F.col("city").rlike("^(Singapore|New York)$"), "EXTENDED").when(F.col("city").rlike("^(Bangkok|Chiang Mai)$"), "PRIORITY").otherwise("GROUP_A").alias("city_zone"),
    )
    .orderBy("country")
)
