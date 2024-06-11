import datetime
import pandas

class completeness:
    def __init__(self, query: object):
        self.query = query
        self.nonsense = "'+', '-', '_','#', '$', '*', '\', '?', '.', '&', '^', '%', '!', '@','NI'"

        