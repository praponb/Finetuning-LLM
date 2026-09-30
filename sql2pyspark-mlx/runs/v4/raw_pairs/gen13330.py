from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "category").orderBy(F.col("revenue").desc(), "id")
w2 = Window.partitionBy("category", "product_id").orderBy("revenue_per_unit_2", "id").rowsBetween(-6, Window.currentRow)

base = (
    df_store_sales
    .filter((F.col("sale_date") >= F.lit("2023-07-06").cast("date")) & F.col("units").isNotNull())
    .select(
        "id",
        "product_id",
        "units",
        "revenue",
        (F.col("revenue") - F.col("units")).alias("revenue_per_unit"),
    )
)

cleaned = (
    base
    .filter(F.col("revenue") > 1200)
    .withColumn("revenue_per_unit_2", (F.col("revenue") * F.col("units")))
)

enriched = (
    cleaned
    .alias("e")
    .join(df_products.alias("b"), F.col("e.product_id") == F.col("b.id"), "inner")
    .select("e.*", F.col("b.launch_date"), F.col("b.category"), F.col("b.price"))
)

enriched_store_sales = (
    enriched
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.avg("revenue_per_unit_2").over(w2).alias("revenue_per_unit_2_avg_6"),
    )
)

result = (
    enriched_store_sales
    .select(
        "id",
        "units",
        "revenue",
        "launch_date",
        "category",
        "row_seq",
        F.when(F.col("category").rlike("^(Grocery|Fashion)$"), "PRIORITY").when(F.col("category").rlike("^(Electronics|Home)$"), "CORE").otherwise("GROUP_B").alias("category_zone"),
    )
)
