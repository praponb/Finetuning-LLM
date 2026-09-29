from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("plan")

flattened = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode_outer("tier_limits").alias("element"))
)

enriched = (
    flattened
    .withColumn("plan_max_element", F.max("element").over(w_grp))
    .withColumn("plan_max_element_2", F.max("element").over(w_grp))
)

base = (
    enriched
    .filter(F.col("element").between(54, 73))
    .select(
        "id",
        "plan",
        "currency",
        "element",
        "plan_max_element",
        "plan_max_element_2",
        F.initcap(F.trim("plan")).alias("plan_title"),
    )
)

result = (
    base
    .select(
        "id",
        "plan",
        "element",
        "plan_max_element",
        "plan_max_element_2",
        "plan_title",
        F.when(F.col("plan").rlike("^(basic|pro)$"), "GROUP_A").otherwise("EXTENDED").alias("plan_zone"),
    )
)
