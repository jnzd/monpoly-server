from db_helper import db_helper

class monitor:
    def __init__(self, sig='', sig_dir='./signatures/', db: db_helper = db_helper()):
        self.signature = sig
        self.db = db
        self.signature_directory = sig_dir
    
    def get_signature(self):
        return self.signature
