import csv 

class CSVWriter(object):
    def __init__(self,file,header=None):
        self.file = file
        self.file_output = open(self.file,'w')
        self.header = header
        self.writer = csv.writer(self.file_output,escapechar='\\')
        if self.header:
            self.writer.writerow(self.header)

    def writerows(self,rows):
        if not self.writer:
            raise Exception("File({}) was already closed".format(self.file))
        if not rows:
            return
        self.writer.writerows(rows)
    
    def writerow(self,row):
        if not self.writer:
            raise Exception("File({}) was already closed".format(self.file))
        if row is None:
            return
        self.writer.writerow(row)

    def write_taskreport(self,task):
        self.writerows(task.reportrows())

    def write_taskwarnings(self,task):
        self.writerows(task.warnings())

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
