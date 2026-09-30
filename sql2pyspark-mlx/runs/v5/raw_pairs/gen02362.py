from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating_whole", "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.launch_date").desc(), F.col("c.id").desc())

filtered = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "*",
        F.ceil("rating").alias("rating_whole"),
        F.concat("country", F.lit(", "), "city").alias("country_city"),
        F.coalesce("city", F.lit("N/A")).alias("city_filled"),
    )
)

deduped = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_latest = (
    deduped
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.cost"))
)

filtered_suppliers = (
    with_latest
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        "city_filled",
        "cost",
        F.upper("supplier_name").alias("supplier_name_upper"),
        F.coalesce("city", F.lit("NONE")).alias("resolved_city"),
    )
)

result = (
    filtered_suppliers
    .select("id", "country", F.col("cost").alias("COST_VAL"), "rating")
    .unpivot(
        ids=["id", "country"],
        values=["COST_VAL", "rating"],
        variableColumnName="metric",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
