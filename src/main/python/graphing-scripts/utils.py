from matplotlib import use, rc
use('Agg')
import matplotlib.pyplot as plt

# plot saving utility function
def writeout(filename_base, formats = ['pdf']):
  for fmt in formats:
    plt.savefig("%s.%s" % (filename_base, fmt), format=fmt, bbox_inches='tight')
#    plt.savefig("%s.%s" % (filename_base, fmt), format=fmt)

def set_leg_fontsize(size):
  rc('legend', fontsize=size)

def set_paper_rcs():
  rc('font',**{'family':'sans-serif','sans-serif':['Helvetica'],
               'serif':['Helvetica'],'size':8})
  rc('text', usetex=True)
  rc('legend', fontsize=7)
  rc('figure', figsize=(3.33,2.22))
#  rc('figure.subplot', left=0.10, top=0.90, bottom=0.12, right=0.95)
  rc('axes', linewidth=0.5)
  rc('lines', linewidth=0.5)

def set_rcs():
  rc('font',**{'family':'sans-serif','sans-serif':['Helvetica'],
               'serif':['Times'],'size':12})
  rc('text', usetex=True)
  rc('legend', fontsize=7)
  rc('figure', figsize=(6,4))
  rc('figure.subplot', left=0.10, top=0.90, bottom=0.12, right=0.95)
  rc('axes', linewidth=0.5)
  rc('lines', linewidth=0.5)

def append_or_create(d, i, e):
  if not i in d:
    d[i] = [e]
  else:
    d[i].append(e)

# Append e to the array at position (i,k).
# d - a dictionary of dictionaries of arrays, essentially a 2d dictionary.
# i, k - essentially a 2 element tuple to use as the key into this 2d dict.
# e - the value to add to the array indexed by key (i,k).
def append_or_create_2d(d, i, k, e):
  if not i in d:
    d[i] = {k : [e]}
  elif k not in d[i]: 
    d[i][k] = [e]
  else:
    d[i][k].append(e)

def cell_to_anon(cell):
  if cell == 'A':
    return 'A'
  elif cell == 'B':
    return 'B'
  elif cell == 'C':
    return 'C'
  elif cell == 'synth':
    return 'SYNTH'
  else:
    print "unknown cell!?"
    raise Exception
