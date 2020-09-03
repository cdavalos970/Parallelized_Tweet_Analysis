# Import mpi so we can run on more than one node and processor
from mpi4py import MPI
# Import csv to read files and arguments
import csv, sys, getopt
# Import regular expressions to look for topics and mentions, json to parse tweet data
import re, json, ijson, operator
from collections import defaultdict
import time
from iso639 import languages
# Define the constants
MASTER_RANK = 0
TREND_TYPE_HASHTAG = 'hashtags'
TREND_TYPE_LANGUAGE = 'language'

def read_arguments(argv):
  # Initialise Variables
  file_name = ''
  trend_type = ''

  ## Read the arguments
  try:
    opts, args = getopt.getopt(argv,"f:tl")
  except getopt.GetoptError as error:
    print(error)
    sys.exit(2)
  for opt, arg in opts:
    if opt in ("-f"):
       file_name = arg
    elif opt in ("-t"):
       trend_type = TREND_TYPE_HASHTAG
    elif opt in ("-l"):
       trend_type = TREND_TYPE_LANGUAGE
       top = arg
  # Return all the arguments
  return file_name, trend_type

def findHash(text, hashDict):
  # Increase the count of hashtag used in the tweet by 1
  for word in text:
    word = word.lower()
    match = re.search('^#[A-z0-9_]*', word)
    if match:
        if word not in hashDict.keys():
          hashDict[word] = 1
        else:
          hashDict[word] = hashDict[word] + 1
  return hashDict


def findLang(lang, langDict):
  # Increase the count of language used in the tweet by 1
  if lang not in langDict.keys():
    langDict[lang] = 1
  else:
    langDict[lang] = langDict[lang] + 1
  return langDict

def process_json_tweets(rank, file_name, processes, trend_type):
  # Open the json file containing all the tweets
  with open(file_name, 'r', encoding = "utf-8") as f:
    objs = ijson.items(f, 'rows.item')
    outDic = {}

    try:
      for i, line in enumerate(objs):
        if i%processes == rank:
          try:
            if trend_type == TREND_TYPE_HASHTAG:
              # Count frequency of hashtags
              tweet = line['doc']["text"]
              tweet = re.split('[!"$%&\'()*+,-./:;<=>?@[\\]^ `{|}~]',tweet)
              #tweet = tweet.split()
              outDic = findHash(tweet,outDic)
            elif trend_type == TREND_TYPE_LANGUAGE:
              # Count frequency of languages
              lang = line["doc"]["metadata"]["iso_language_code"]
              try:
                lang = languages.get(alpha2=lang).name + "(" + lang + ")"
              except KeyError:
                lang = "Undefined" + "(" + lang + ")"
              outDic = findLang(lang, outDic)
          except ValueError:
            print("Malformed JSON in tweet", i)
          except:
            print("Unexpected error:", sys.exc_info()[0])
            raise
    except TypeError:
      print("Could not read line in json.")
  return outDic

def consolidate_slave_data(comm):
  processes = comm.Get_size()
  counts = []
  # Send requests to slave processors to send the data
  for i in range(processes-1):
    # Send the request
    comm.send('return_data', dest=(i+1), tag=(i+1))
  for i in range(processes-1):
    # Receive data from slave processors
    counts.append(comm.recv(source=(i+1), tag=MASTER_RANK))
  return counts

def print_top_trends(hashtags, trend_type, top):
  # This is a common method to print top trends
  i=0
  sorted_counts = sorted(hashtags.items(), key=operator.itemgetter(1))
  print("The top %s trending %s are:" % (top, trend_type))
  for hashtag in reversed(sorted_counts[-top:]):
    hashtag, times = hashtag
    i+=1
    print(i,".",hashtag,",", times)

def master_data_processor(comm, file_name, trend_type):
    # Get rank and size
    rank = comm.Get_rank()
    size = comm.Get_size()

    occurences = process_json_tweets(rank, file_name, size, trend_type)
    if size > 1:
      counts = consolidate_slave_data(comm)
      # Consolidate received data
      for d in counts:
        for k,v in d.items():
          occurences[k] = occurences.setdefault(k,0) + v
      # Send communications to slave processors to finish the tasks
      for i in range(size-1):
        comm.send('exit', dest=(i+1), tag=(i+1))
    # Print top 10 trends
    print_top_trends(occurences, trend_type,10)

def slave_data_processor(comm, file_name, trend_type):
  # Get the current processor rank and size
  rank = comm.Get_rank()
  size = comm.Get_size()

  counts = process_json_tweets(rank, file_name, size, trend_type)
  # Wait for a communication from master processor to send the processed data
  while True:
    in_comm = comm.recv(source=MASTER_RANK, tag=rank)
    # Check if command
    if isinstance(in_comm, str):
      if in_comm in ("return_data"):
        # Send data to master processor
        print("Process: ", rank, " sending back ", len(counts), " items")
        comm.send(counts, dest=MASTER_RANK, tag=MASTER_RANK)
      elif in_comm in ("exit"):
        exit(0)


def main(argv):
  # Start the timer
  start_time = time.time()
  # Interpret the command arguments
  file_name, trend_type = read_arguments(argv)

  # Identify whether the current node is a master processor or slave processor
  comm = MPI.COMM_WORLD
  rank = comm.Get_rank()

  print("This message is sent from rank %s." % (rank))
  if rank == 0 :
    # Rank 0 indicates that the current processor a master processor
    master_data_processor(comm, file_name, trend_type)
  else:
    # Rank>0 indicates a current processor is a slave processor
    slave_data_processor(comm, file_name, trend_type)

  # Stop the timer
  total_time = time.time() - start_time
  print("The total time of the execution is: %s" % (total_time))

# This method gets invoked first when executed from slurm job
if __name__ == "__main__":
    main(sys.argv[1:])
