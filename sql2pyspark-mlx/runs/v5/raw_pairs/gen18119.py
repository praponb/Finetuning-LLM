from pyspark.sql import functions as F

base = (
    df_store_sales
    .filter(F.col("sale_date") > F.lit("2023-04-27").cast("date"))
    .withColumn("sale_date_label", F.date_format("sale_date", "MMM yyyy"))
)

result = (
    base
    .alias("s")
    .join(df_stores.alias("p2"), F.col("s.store_id") == F.col("p2.id"), "inner")
    .filter(F.col("p2.sqft") < 8850)
    .select(
        "s.id",
        "s.store_id",
        "s.product_id",
        "s.sale_date",
        "s.units",
        "s.sale_date_label",
        F.col("p2.store_name"),
    )
)
