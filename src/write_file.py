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
      print filename_
      if os.path.exists(filename_):
        f= open(filename_, "r+")
      else:
        f= open(filename_, "w")
      lines = f.read().splitlines()
      for content_ in self.content :
        if content_ not in lines:
          linewrite = content_+"\n"
          f.write(linewrite)
      f.close()
    except:
        print "__Except write file__"