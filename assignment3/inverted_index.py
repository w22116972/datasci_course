import MapReduce
import sys

mr = MapReduce.MapReduce(sys.argv[0])


def mapper(doc):
    '''
    The input is a 2-element list: [document_id, text], where document_id is a string representing a document identifier and text is a string representing the text of the document. The document text may have words in upper or lower case and may contain punctuation. You should treat each token as if it was a valid word; that is, you can just use value.split() to tokenize the string.
    '''
    key = doc[0]  # doc ID
    text = doc[1]
    words = text.split()
    for w in words:
        mr.emit_intermediate(w, key)
# The output should be a (word, document ID list) tuple where word is a String and document ID list is a list of Strings.


def reducer(key, list_of_docID):
    list_of_id = []
    for i in list_of_docID:
        list_of_id.append(i)
    mr.emit((key, list_of_id))

if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)
