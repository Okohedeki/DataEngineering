import sparknlp
# Start Spark Session
spark = sparknlp.start()
import pandas as pd 
from sparknlp.base import LightPipeline


# Import the required modules and classes
from sparknlp.base import DocumentAssembler, Pipeline
from sparknlp.annotator import (
    SentenceDetector,
    Tokenizer,
    YakeKeywordExtraction
)
import pyspark.sql.functions as F
# Step 1: Transforms raw texts to `document` annotation
document = DocumentAssembler() \
            .setInputCol("text") \
            .setOutputCol("document")
# Step 2: Sentence Detection
sentenceDetector = SentenceDetector() \
            .setInputCols("document") \
            .setOutputCol("sentence")
# Step 3: Tokenization
token = Tokenizer() \
            .setInputCols("sentence") \
            .setOutputCol("token") \
            .setContextChars(["(", ")", "?", "!", ".", ","])
# Step 4: Keyword Extraction
keywords = YakeKeywordExtraction() \
            .setInputCols("token") \
            .setOutputCol("keywords") \
            
# Define the pipeline
yake_pipeline = Pipeline(stages=[document, sentenceDetector, token, keywords])
# Create an empty dataframe
empty_df = spark.createDataFrame([['']]).toDF("text")
# Fit the dataframe to get the 
yake_Model = yake_pipeline.fit(empty_df)

light_model = LightPipeline(yake_Model)

text = '''
Skin cancer is the most commonly diagnosed cancer worldwide. Understanding the natural history of skin cancer will provide a framework for the creation of prevention 
and control strategies that aim to reduce skin cancer burden. The strategies include health promotion, primary prevention, secondary prevention, and tertiary prevention. 
Health promotion and primary prevention were covered in the first part of this 2-part review. The second part covers the secondary and tertiary prevention of skin cancer. In particular, 
preventive strategies centered on the early detection of skin cancer, the prevention of disease progression, clinical surveillance, and educational and behavioral interventions are highlighted. 
The summaries of existing recommendations, challenges, opportunities, and future directions are discussed.
'''

light_result = light_model.fullAnnotate(text)[0]


keys_df = pd.DataFrame([(k.result, k.begin, k.end, k.metadata['score'],  k.metadata['sentence']) for k in light_result['keywords']],
                       columns = ['keywords','begin','end','score','sentence'])
keys_df['score'] = keys_df['score'].astype(float)
# ordered by relevance 
print(keys_df.sort_values(['sentence','score']).head(100))