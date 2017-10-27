import sys
import os
class File_RW(object):
  def __init__(self,file_name,content):
    self.file_name = file_name
    self.content = content
  def writefile(self):
    try:
      path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
      filename_ = path+"/output/"+str(self.file_name)
      if os.path.exists(filename_):
        f= open(filename_, "w")
      else:
        f= open(filename_, "w")
      for content_ in self.content :
          f.write(content_+'\n')
      f.close()
    except:
        print "__Except write file__"

