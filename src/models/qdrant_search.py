from qdrant_client import models, QdrantClient
from src.models.mysql_connector import chat_bot_execute_query
import os
import openai

class QdrantSearch:
    def __init__(self):
        self.qdrant_url = os.environ.get('QDRANT_HOST')
        self.client = QdrantClient(self.qdrant_url,grpc_port=6334, prefer_grpc=True) 
        self.openai_client = openai.Client(api_key=os.environ.get('OPENAI_API_KEY'))

    def api_json_response_format(self,status,message,error_code,data):
        result_json = {"success" : status,"message" : message,"error_code" : error_code,"data": data}
        return result_json

    def create_embedding(self,service_id):
        is_prod_desc_embd = 'N'
        is_end_child_embd = 'N'
        service_name = ''
        query = "SELECT is_prod_desc_embd,is_end_child_embd FROM client_service WHERE service_id = %s" 
        value = (service_id,)       
        res_obj = chat_bot_execute_query(query,value)        
        if res_obj:
            for res in res_obj:
                is_prod_desc_embd = res["is_prod_desc_embd"]
                is_end_child_embd = res["is_end_child_embd"]

        if is_end_child_embd == "N":
            query = "SELECT p.product_id,p.parent_product_id,p.service_id,cs.service_name,p.product_name,p.product_desc FROM product p LEFT JOIN client_service AS cs ON cs.service_id = p.service_id WHERE p.service_id = %s AND ( p.product_id IN (SELECT parent_product_id FROM product WHERE parent_product_id IS NOT NULL) OR p.parent_product_id IS NULL );"
        else:
            query = "SELECT p.parent_product_id,p.product_id,p.service_id,cs.service_name,p.product_name,p.product_desc FROM product AS p LEFT JOIN client_service AS cs ON p.service_id = cs.service_id WHERE p.service_id = %s" 

        value = (service_id,)       
        category_obj = chat_bot_execute_query(query,value)
        product_category_details = []
        if category_obj:
            for category in category_obj:
                product_category_details.append({                    
                    "service_name": str(category['service_name']),
                    "parent_product_id": str(category['parent_product_id']),
                    "product_id": str(category['product_id']),                    
                    "product_name": category['product_name'],
                    "product_desc": str(category['product_desc'])+""
                })
                service_name = str(category['service_name'])

            try:
                existing_collections = self.client.get_collections().collections
                collection_names = [collection.name for collection in existing_collections]

                if "product_list_by_service_"+service_name in collection_names:
                    self.client.delete_collection(collection_name="product_list_by_service_"+service_name)

                # Create or recreate the collection in Qdrant with HNSW configuration
                self.client.create_collection(
                    collection_name="product_list_by_service_"+service_name,
                    vectors_config=models.VectorParams(
                        size=1536,  # Vector size based on the encoder model
                        distance=models.Distance.COSINE,  # Distance metric for similarity search
                        hnsw_config=models.HnswConfigDiff(
                            ef_construct=200,  # Controls index construction speed vs. accuracy tradeoff
                            m=16,  # Number of bi-directional links created for every new element during construction
                            full_scan_threshold=10000  # Threshold for switching between HNSW and brute-force search
                        )
                    ),
                )


                if is_prod_desc_embd == "Y":
                    embed_texts = [
                                    f"{product_details['product_name']}, {product_details['product_desc']}"
                                    for product_details in product_category_details
                                ]
                else:
                    embed_texts = [
                                    f"{product_details['product_name']}"
                                    for product_details in product_category_details
                                ]
                
                response = self.openai_client.embeddings.create(
                                input=embed_texts,
                                model=str(os.environ.get('EMBEDDING_MODEL'))
                            )
                
                self.client.upload_points(
                    collection_name="product_list_by_service_"+service_name,
                    points=[
                            models.PointStruct(
                                id=idx,
                                vector=data.embedding,
                                payload=text,
                            )for idx, (data, text) in enumerate(zip(response.data, product_category_details))
                    ]
                )
                
                print("Products embedded and uploaded successfully!")
                return self.api_json_response_format(True,str("Product category embedded successfully!"),200,{})

            except Exception as e:
                print(f"Error in products embeddeding. Error : {e}")
                return self.api_json_response_format(False,str("Error in products embeddeding. Error : ",e),500,{})

    def search_products(self,query,service_name,search_limit,search_thersold):        
        response = self.openai_client.embeddings.create(
                        input=query,
                        model=str(os.environ.get('EMBEDDING_MODEL'))
                    )
        query_vector = response.data[0].embedding
        # Perform the search with filter
        print(f"qdrant query : {query}")
                
        filter_condn = [
                models.FieldCondition(
                    key="service_name",
                    match=models.MatchValue(value=str(service_name))
                )]
            
        hits = self.client.search(
            collection_name="product_list_by_service_"+str(service_name),
            query_vector=query_vector,
            limit=search_limit,  # Limit the number of results
            search_params=models.SearchParams(
                hnsw_ef=128  # Increase for better accuracy, decrease for faster response                
            ),
            query_filter=models.Filter(
                must=filter_condn
            )
        )
        search_result = []
        for hit in hits:
            print(f"qdrant result {hit} hit-payload : {hit.payload}, hit-score : {hit.score}")
            if hit.payload["service_name"] == str(service_name):
                if hit.score > search_thersold:
                    prod_categories = {}
                    prod_categories["payload"] = hit.payload
                    prod_categories["score"] = hit.score                    
                    return self.api_json_response_format(True,str("Product category found."),200,prod_categories)                    
                else:
                    prod_categories = {}
                    prod_categories["payload"] = hit.payload
                    prod_categories["score"] = hit.score                    
                    search_result.append(prod_categories)

        return self.api_json_response_format(False,str("Product category not found."),404,search_result)
