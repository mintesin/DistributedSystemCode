"""
Implements OnRequestCallback interface to handle search tasks
"""

import os
import pickle
from model.document_data import DocumentData
from model.result import Result
from tfidf import TFIDF  # Corrected import for TFIDF


class SearchWorker:
    ENDPOINT = "/task"

    def handle_request(self, request_payload: bytes) -> bytes:
        task = pickle.loads(request_payload)  # Deserialize Task object
        result = self.create_result(task)
        return pickle.dumps(result)  # Serialize Result object

    def create_result(self, task) -> Result:
        documents = task.get_documents()
        print(f"Received {len(documents)} documents to process")

        result = Result()

        for document in documents:
            words = self.parse_words_from_document(document)
            document_data = TFIDF.create_document_data(words, task.get_search_terms())
            result.add_document_data(document, document_data)

        return result

    def parse_words_from_document(self, document: str):
        if not os.path.exists(document):
            return []
        with open(document, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        words = TFIDF.get_words_from_document(lines)
        return words

    def get_endpoint(self) -> str:
        return self.ENDPOINT
