"""
Module with functions to calculate TF-IDF scores for documents
"""

import math
from collections import defaultdict
from model.document_data import DocumentData


def calculate_term_frequency(words, term):
    count = sum(1 for word in words if word.lower() == term.lower())
    term_frequency = count / len(words) if words else 0
    return term_frequency


def create_document_data(words, terms):
    document_data = DocumentData()
    for term in terms:
        term_freq = calculate_term_frequency(words, term.lower())
        document_data.put_term_frequency(term, term_freq)
    return document_data


def get_documents_scores(terms, document_results):
    score_to_doc = defaultdict(list)
    term_to_idf = get_term_to_inverse_document_frequency_map(terms, document_results)

    for document, document_data in document_results.items():
        score = calculate_document_score(terms, document_data, term_to_idf)
        score_to_doc[score].append(document)

    # Return scores sorted descending by score
    return dict(sorted(score_to_doc.items(), key=lambda item: item[0], reverse=True))


def calculate_document_score(terms, document_data, term_to_idf):
    score = 0
    for term in terms:
        term_frequency = document_data.get_frequency(term)
        inverse_term_frequency = term_to_idf.get(term, 0)
        score += term_frequency * inverse_term_frequency
    return score


def get_inverse_document_frequency(term, document_results):
    n = sum(1 for document_data in document_results.values() if document_data.get_frequency(term) > 0)
    if n == 0:
        return 0
    return math.log10(len(document_results) / n)


def get_term_to_inverse_document_frequency_map(terms, document_results):
    term_to_idf = {}
    for term in terms:
        idf = get_inverse_document_frequency(term, document_results)
        term_to_idf[term] = idf
    return term_to_idf


def get_words_from_document(lines):
    words = []
    for line in lines:
        words.extend(get_words_from_line(line))
    return words


def get_words_from_line(line):
    import re
    # Split on punctuation and whitespace
    return re.split(r"[.,\s\-?!;:/\d\n]+", line)
