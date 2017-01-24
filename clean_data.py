import json;
import codecs;

if __name__ == '__main__':
  l = set();
  with codecs.open('out', 'r', 'utf-8') as infile:
    for line in infile:
      j = json.loads(line);
      l.add(j['id']);
  # print(l);
  with codecs.open('counties', 'a', 'utf-8') as f:
    for elem in l:
#      print(elem);
      f.write(elem);
      f.write('\n');
    
