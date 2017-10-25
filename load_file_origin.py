import sys
class File_origin(object):
	def __init__(self,path):
		self.path =path
	def remove_char(self,origin):
		char =[' ','\t','=','>',',',']']
		for i in char:
			origin =origin.replace(i,'')
		return origin
	def read_file(self):
		f= open(self.path, "r")
		lines = f.read().splitlines()
		result =[]
		for line in lines:
			if line and(line[0] is not '#'):
				line_ = self.remove_char(line)
				result.append(line_.split('['))
		return result

