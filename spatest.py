#from __future__ import print_function
import re
import sys
from operator import add

from pyspark import SparkContext

ADCPC = "/home/hncuong/cache/c71/adv*"

def adid(url):
  urlre = re.split(r"[-.]", url)
  return urlre[len(urlre) - 2]

def lineStrip(line):
  sline = line.split("\t")
  guid = sline[14]
  woc = 1
#  woc =
  aid = adid(sline[10]) + "\t"
  return (guid, (woc, aid))



def main():
  if len(sys.argv) != 1:
    print("Usage:  <file>")
    exit(-1)
  sc = SparkContext(appName = "User click")
  lines = sc.textFile(ADCPC)
  mclines = lines.filter(lambda line: "muachung.vn" in line)
  print(mclines.count())
  mclines = mclines.map(lineStrip)
  mclines = mclines.reduceByKey(lambda x,y: (x[0] + y[0], x[1] + y[1]))
  print(mclines.count())
  output = mclines.take(10)
#  woc = 1
#  print(mclines.first())
#  print(mclines.count())
#  word = "hehe"
#  count = 123
#  print("User %s: has %i" % (word, count))
  for (guid, (woc, aid)) in output:
    print("User %s: has %s such as " % (guid, woc ))
    print aid


#  print ("User:  has %d click.Eg: ", % (woc))
#  for (guid, (woc, aid)) in output:
#    print "User:  has %i click.Eg: ", % (woc)
  sc.stop()
if __name__ == "__main__":

    main()
