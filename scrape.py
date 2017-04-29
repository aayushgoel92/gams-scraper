import requests;
import urllib.parse;
import time;
from selenium import webdriver;
from multiprocessing import Process, Lock, Pool;
import codecs;
from os.path import join, exists;
from os import makedirs, listdir, remove;
import pandas as pd;

def get_table_for_county(info):
  crop = info[0].strip();
  id = info[1].strip();
  driver = webdriver.Firefox(executable_path='/Users/aayush/Developer/gams-scraper.git/geckodriver');
  driver.set_page_load_timeout(10);
  print('firefox started');
  try:
    page_url = 'https://glam1.gsfc.nasa.gov/wui/3.1.0/chart.html?sat=MYD&mean=2003-2015&layer=NDVI&crop={}&type=ADM&n=1&numIds=1&level=3&initYr=2017&id0={}';
    id0 = urllib.parse.quote_plus(id);
    driver.get(page_url.format(crop, id0));
    table = driver.find_element_by_id("table_download").get_attribute('href');
  except:
    table = "";
  driver.quit();
  print('firefox quit');
  return (id, table);

def fetch_files(output_dir, links):
  if not exists(output_dir):
    makedirs(output_dir);
  for link in links:
    if link[1] == "":
      continue;
    print("Fetching " + link[1]);
    resp = requests.get(link[1]);
    split_str = link[0].split('|');
    fname = split_str[0];
#    fname = "_".join([split_str[0], split_str[4], split_str[6]]);
    fname = join(output_dir, fname);
    table = resp.text;
    table_rows = table.split('\n');
    table_rows = table_rows[12:];
    with codecs.open(fname, 'w', 'utf-8') as f:
      f.write('\n'.join(table_rows).strip());

def consolidate_csv(data_dir, outfile):
  headings = [ 'SAMPLE VALUE', 'SAMPLE COUNT', 'MEAN VALUE', 'MEAN COUNT' ]
  data_files = listdir(data_dir);
  data = {};
  for fname in data_files:
    if fname[0] == '.':
      continue;
    data[fname] = pd.read_csv(join(data_dir, fname));
    print(data[fname]);
    l = len(data[fname]['START DATE']);
    data[fname]['COUNTY ID'] = pd.Series([int(fname)]*l);
  master = pd.concat(list(data.values()));
  for head in headings:
    table1 = pd.pivot_table(master[['ORDINAL DATE', 'COUNTY ID', head ]], index='ORDINAL DATE', columns='COUNTY ID');
    crop = outfile.split('.')[0];
    with codecs.open( crop + '_' + head.replace(' ', '_') + '.csv', 'w', 'utf-8') as f:
      table1.to_csv(f);
  with codecs.open(outfile, 'w', 'utf-8') as f:
    master.to_csv(f, index=False);

if __name__ == '__main__':

  crops = ['NASS_2010_soybeans', 'NASS_2010_cotton', 'NASS_2010_winter-wheat', 'NASS_2010_corn'];
  counties = [];
  with codecs.open('counties', 'r', 'utf-8') as f:
    for line in f:
      splt = line.split('|');
      if splt[2] == 'United States of America':
        counties.append(line)
  for crop in crops:
    info = [];
    for county in counties:
      info.append((crop, county));
    pool = Pool(processes=4);
    res = pool.map(get_table_for_county, info);
    pool.close();
    pool.join();
    with codecs.open('file_links', 'a', 'utf-8') as f:
      for link in res:
        link_str = ",".join(link);
        f.write(link_str + "\n");
    fetch_files('data', res);
    remove('file_links');
    consolidate_csv('data', crop + '.csv');
