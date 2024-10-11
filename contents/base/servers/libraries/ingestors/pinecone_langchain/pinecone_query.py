import logging
from langchain.llms import OpenAI
from langchain.chains import RetrievalQA
from langchain import PromptTemplate, LLMChain
from langchain.chains.question_answering import load_qa_chain
from langchain.vectorstores.pinecone import Pinecone
from langchain.retrievers import PineconeHybridSearchRetriever


class Query:

    def __init__(self, api_key, environment, index_name, embeddings, openai_key):
        logging.basicConfig(
            format="%(asctime)s %(levelname)s: %(message)s", level=logging.INFO
        )
        self.api_key = api_key
        self.environment = environment
        self.index_name = index_name
        self.embeddings = embeddings

        self.openai_key = openai_key

        # self.indexer = Indexer(api_key, environment, index_name, embeddings)
        self.logger = logging.getLogger()

    # doc store similarity search
    def search(self, query, k=3):

        #print(self.index.describe_index_stats())
        docsearch = Pinecone.from_existing_index(self.index_name, self.embeddings)
        return docsearch.similarity_search(query, k=k)

    # search with scores
    def search_with_relevance(self, query, k=3):

        #print(self.index.describe_index_stats())
        docsearch = Pinecone.from_existing_index(self.index_name, self.embeddings)
        return docsearch.similarity_search_with_relevance_scores(query, k=k)

    def search_with_openai(self, query, k=3):

        llm = OpenAI(temperature=0, openai_api_key=self.openai_key)
        chain = load_qa_chain(llm, chain_type="stuff")

        docs = self.search(query, k)

        result = chain.run(input_documents=docs, question=query)
        print(result)

    # def retrieval_with_openai(self, query, k=3):

    #     splade_encoder = SpladeEncoder()
    #     retriever = PineconeHybridSearchRetriever(embeddings=self.vi.embeddings,
    #                  sparse_encoder=splade_encoder, index=self.vi.index_name)

    #     llm = OpenAI(temperature=0, openai_api_key=self.openai_key)

    #     qa = RetrievalQA.from_chain_type(llm, chain_type="stuff", retriever=retriever)

    #     result = qa.run(query)
    #     print(result)


    def openai(self, template, variables, query, k=3):

        llm = OpenAI(temperature=0, openai_api_key=self.openai_key)

        prompt = PromptTemplate(template=template, input_variables=variables)

        llm_chain = LLMChain(prompt=prompt, llm=llm)

        result = llm_chain.run(query)
        print(result)