"""
Python equivalent of DocumentData.java
Data model class to store term frequencies for a document
"""

class DocumentData:
    def __init__(self):
        self.term_to_frequency = {}
    # aggretagting the key -value ssyteml of words to the their tf
    def put_term_frequency(self, term, frequency):
        self.term_to_frequency[term] = frequency

    def get_frequency(self, term):
        return self.term_to_frequency.get(term, 0)
