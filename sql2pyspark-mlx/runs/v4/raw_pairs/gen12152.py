from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("country").orderBy("rating", "id")

cleaned = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.when(F.col("country").isin(["US", "DE"]), "Tier 1").otherwise("Other").alias("country_group"),
    )
)

cleaned_suppliers = (
    cleaned
    .filter(F.col("city") == "London")
    .select("*", F.trim("country").alias("clean_country"))
)

metrics = (
    cleaned_suppliers
    .withColumn("running_rating", F.sum("rating").over(w))
    .withColumn("rating_bucket", F.ntile(10).over(w2))
)

result = (
    metrics
    .select(
        "id",
        "supplier_name",
        "country",
        "country_group",
        "clean_country",
        "running_rating",
        F.when(F.col("supplier_name").rlike("^(Global Supply|Lion City Imports)$"), "EXTENDED").when(F.col("supplier_name").rlike("^(Siam Parts Co|Tokyo Traders)$"), "PRIORITY").otherwise("GROUP_A").alias("supplier_name_zone"),
    )
    .orderBy(F.col("id").desc_nulls_first())
    .limit(20)
)
