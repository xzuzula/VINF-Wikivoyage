import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# spark-submit --packages com.databricks:spark-xml_2.12:0.15.0 --master local PySpark.py

infobox_regex = r"\n\|(.*?)=(.*)"
dump_file = "E:\\VINF_data\\enwiki-20230820-pages-articles-multistream.xml"

def load_gazetteer(path="src_code/gazetteer.txt") -> list[str]:
	"""
	Loads a list of entities from a specified text file.

	This function reads a text file line by line and appends each stripped line to a list, which it then returns. It is primarily used for loading a list of entities, such as names or keywords, from a file.

	Parameters:
	path (str): The file path to read the entities from. Defaults to "src_code/gazetteer.txt".

	Returns:
	list[str]: A list of entities read from the file.
	"""
	entities = []
	with open(path, "r", encoding='utf-8') as txt_file:
		for line in txt_file:
			entities.append(line.strip())
	return entities

def clean_text(data: list[str]) -> list[str]:
	"""
    Cleans a list of text strings by stripping and removing unwanted characters.

    This function takes a list of text strings and performs several cleaning operations. It strips whitespace, removes special characters and HTML tags, and replaces multiple spaces with a single space.

    Parameters:
    data (list[str]): A list of strings to be cleaned.

    Returns:
    list[str]: The cleaned list of strings.
    """
	new_data = []
	for item in data:
		new_text = item.strip()
		new_text = re.sub(r"[\[\]\{\}\=]|<.*?>", "", new_text)
		new_text = re.sub(r"\|", " ", new_text)
		new_text = re.sub(r" +", " ", new_text)
		new_data.append(new_text)
	return new_data

def create_session() -> SparkSession:
	"""
    Creates and returns a new SparkSession.

    This function initializes and returns a SparkSession with a specified application name. It is intended for use in a Spark application for processing large datasets.

    Returns:
    SparkSession: A newly created SparkSession.
    """
	spark_sess = SparkSession.builder.appName("regex_wiki").getOrCreate()
	return spark_sess

# Load gazetteer and init spark
gazetteer = load_gazetteer()
spark = create_session()
clean_data = udf(clean_text, ArrayType(StringType()))

# Load XML file
df = spark.read.format("com.databricks.spark.xml").options(rootTag="mediawiki", rowTag="page", encoding="ISO-8859-1").load(dump_file)
df = df.filter(col("title").isin(gazetteer))

# Extract infobox keys and values
extracted_df = df.withColumns({"reg_key": regexp_extract_all('revision.text._VALUE', lit(infobox_regex), 1),
							   "reg_value": regexp_extract_all('revision.text._VALUE', lit(infobox_regex), 2)})

# Filter results
ne_df = extracted_df.filter(size(col("reg_key")) > 0)
mod_df = ne_df.withColumns({"mod_key": clean_data(ne_df.reg_key),
							"mod_val": clean_data(ne_df.reg_value)})
final_data = mod_df.select("title", "mod_key", "mod_val")

# Write results to file
final_data.write.mode("overwrite").json("spark_output")

# End spark session
spark.stop()
