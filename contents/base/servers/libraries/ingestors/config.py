class Config:
    def __init__(self):
        self.s3client = None
        self.session = None
        self.page_size = 10
        self.search_query = None

    def set_s3client(self, s3client):
        self.s3client = s3client
        return self

    def set_session(self, session):
        self.session = session
        return self

    def set_page_size(self, page_size):
        self.page_size = page_size
        return self

    def set_search_query(self, search_query):
        self.search_query = search_query
        return self

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass