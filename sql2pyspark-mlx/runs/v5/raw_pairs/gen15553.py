from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city").orderBy(F.col("rating").desc_nulls_first(), "id")

filtered = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "country",
        "city",
        "rating",
        F.concat("city", F.lit(" "), "supplier_name").alias("city_supplier_name"),
    )
)

ranked = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "country", "city_supplier_name")
)

base = (
    ranked
    .filter(F.col("country").isin(["DE", "SG", "TH"]))
    .withColumn("country_lower", F.lower("country"))
)

kept = (
    base
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.supplier_id") == 27), "left_anti")
)

summary = (
    kept
    .groupBy("country")
    .agg(
        F.count(F.when(F.col("country") == "US", 1)).alias("us_count"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.count(F.when(F.col("country") == "US", 1)).alias("us_count_2"),
    )
)

result = (
    summary
    .select(
        "country",
        "country_list",
        F.when(F.col("country").rlike("^(UK|JP)$"), "CORE").when(F.col("country").rlike("^(TH|US)$"), "GROUP_B").otherwise("GROUP_A").alias("country_zone"),
    )
)
