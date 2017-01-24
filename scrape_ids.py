import requests;
import copy;
import json;
from multiprocessing import Process, Lock, Pool;

def get_id_info(info):
  global sess;
  url = 'http://glam1.gsfc.nasa.gov/app/2.3/idshp?get=admPC&level=G2014_2013_2_mid&z={}&x={}&y={}&crop={}&shape=ADM'
  r = sess.get(url.format(info['z'], info['x'], info['y'], info['crop']));
  result = copy.copy(info);
  if 'error' not in r.text:
    result['id'] = r.text;
    lock.acquire();
    with open('out', 'a') as f:
      json.dump(result, f);
    lock.release();
  del result;
  return(True);

def init(l):
  global lock
  lock = l

if __name__ == '__main__':
  crops = ['NASS_2010_corn'];
  zoomlevel = 6;
  x = range(2500, 5100);
  y = range(5500, 7000);
  sess = requests.Session();
  inputs = [];

  for crop in crops:
    for a in x:
      for b in y:
        # r = sess.get(url.format(zoomlevel, a, b, crop));
        # print(r.text);
        inputs.append({ "z": zoomlevel, "x": a, "y": b, "crop": crop});
  
  l = Lock();
  pool = Pool(processes=2, initializer=init, initargs=(l,));
  pool.map(get_id_info, inputs);
  pool.close();
  pool.join();

