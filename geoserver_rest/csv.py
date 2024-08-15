import csv 

class CSVWriter(object):
    def __init__(self,file,header=None):
        self.file = file
        self.file_output = open(self.file,'w')
        self.header = header
        self.writer = csv.writer(self.file_output,escapechar='\\')
        self.records = 0
        if self.header:
            self.writer.writerow(self.header)

    def writerows(self,rows):
        if not self.writer:
            raise Exception("File({}) was already closed".format(self.file))
        if not rows:
            return
        for row in rows:
            self.writerow(row)
    
    def writerow(self,row):
        if not self.writer:
            raise Exception("File({}) was already closed".format(self.file))
        if row is None:
            return
        self.records += 1
        self.writer.writerow(row)

    def close(self):
        try:
            if self.file_output:
                self.file_output.close()
        except:
            pass
        self.file_output = None
        self.writer = None

    def __enter__(self):
        return self

    def __exit__(self,t,value,tb):
        self.close()
        return False if value else True
