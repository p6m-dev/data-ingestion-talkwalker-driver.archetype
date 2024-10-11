import logging
import pinecone
from langchain.embeddings import HuggingFaceEmbeddings
from langchain.vectorstores.pinecone import Pinecone
from langchain.text_splitter import CharacterTextSplitter
from langchain.vectorstores.pinecone import Pinecone
from langchain.document_loaders import (
    CSVLoader,
    EverNoteLoader,
    PyMuPDFLoader,
    TextLoader,
    DirectoryLoader,
    UnstructuredEmailLoader,
    UnstructuredEPubLoader,
    UnstructuredCSVLoader,
    UnstructuredHTMLLoader,
    UnstructuredMarkdownLoader,
    UnstructuredODTLoader,
    UnstructuredPowerPointLoader,
    UnstructuredWordDocumentLoader,
)


class Indexer:

    def __init__(self, api_key, environment, index_name, embeddings):

        logging.basicConfig(
            format="%(asctime)s %(levelname)s: %(message)s", level=logging.INFO
        )

        self.logger = logging.getLogger()
        self.api_key = api_key
        self.environment = environment
        self.index_name = index_name
        self.embeddings = embeddings

        # pinecone.init(api_key=constants.PINECONE_API_KEY,
        #               environment=constants.PINECONE_ENVIRONMENT)

    def create(self, dimension=768, metric="cosine", shards=1, replicas=1):
        # time consuming operation
        pinecone.create_index(self.index_name, dimension=dimension,
                              metric=metric, shards=shards,
                              replicas=replicas)

    def delete(self):
        # time consuming operation
        pinecone.delete_index(self.index_name)

    def add_string(self, texts, metadatas=None, ids=None):
        # time consuming operation
        vectorstore = Pinecone.from_existing_index(self.index_name,
                                                   self.embeddings,
                                                   "text")
        # texts is an array of strings
        vectorstore.add_texts(texts, metadatas=metadatas, ids=ids)

    def add_text_file(self, file_path, metadatas=None, ids=None):
        # time consuming operation
        vectorstore = Pinecone.from_existing_index(self.index_name,
                                                   self.embeddings,
                                                   "text")

        loader = TextLoader(file_path)
        # returns a Langchain Document array with Document(page_content=text)
        documents = loader.load()

        text_splitter = CharacterTextSplitter(chunk_size=1000, chunk_overlap=0)

        # returns a Langchain Document array with Document(page_content=text, metadata=metadata)
        docs = text_splitter.split_documents(documents)

        # print(docs[0].page_content)

        # expects a list of strings
        vectorstore.add_texts([doc.page_content for doc in docs], metadatas=metadatas, ids=ids)

    def add_html_file(self, file_path, metadatas=None, ids=None):
        # time consuming operation
        vectorstore = Pinecone.from_existing_index(self.index_name,
                                                   self.embeddings,
                                                   "text")

        loader = UnstructuredHTMLLoader(file_path)
        # returns a Langchain Document array with Document(page_content=text)
        documents = loader.load()

        text_splitter = CharacterTextSplitter(chunk_size=1000, chunk_overlap=0)

        # returns a Langchain Document array with Document(page_content=text, metadata=metadata)
        docs = text_splitter.split_documents(documents)

        # print(docs[0].page_content)

        # expects a list of strings
        vectorstore.add_texts([doc.page_content for doc in docs], metadatas=metadatas, ids=ids)

    def add_csv_file(self, file_path, metadatas=None, ids=None):
        # time consuming operation
        vectorstore = Pinecone.from_existing_index(self.index_name,
                                                   self.embeddings,
                                                   "text")

        loader = UnstructuredCSVLoader(file_path)  # , mode="elements")
        # returns a Langchain Document array with Document(page_content=text)
        documents = loader.load()

        text_splitter = CharacterTextSplitter(chunk_size=256, chunk_overlap=0)

        # returns a Langchain Document array with Document(page_content=text, metadata=metadata)
        docs = text_splitter.split_documents(documents)

        # print(docs[0].page_content)

        # expects a list of strings
        vectorstore.add_texts([doc.page_content for doc in docs], metadatas=metadatas, ids=ids)

    def add_dir(self, file_path):
        # time consuming operation
        vectorstore = Pinecone.from_existing_index(self.index_name,
                                                   self.embeddings,
                                                   "text")

        loader = DirectoryLoader(file_path, glob='**/*.*', recursive=True)
        # returns a Langchain Document array with Document(page_content=text)
        documents = loader.load()

        text_splitter = CharacterTextSplitter(chunk_size=1000, chunk_overlap=0)

        # returns a Langchain Document array with Document(page_content=text, metadata=metadata)
        docs = text_splitter.split_documents(documents)

        # print(docs[0].page_content)

        # expects a list of strings
        vectorstore.add_texts([doc.page_content for doc in docs])  # , metadatas=metadatas, ids=ids)