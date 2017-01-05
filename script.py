from os import system

dir_name = 'src/ru/mipt/examples'

from os.path import join

inputs = ['/data/wiki/en_articles_part temp']
executables = ['WordCount']

from sys import argv

silent = False
if len(argv) > 1 and argv[1] == '-s':
  silent = True

def execute_command(command):
  print '[***] ' + command
  if silent == True: return
  if system(command):
    raise Exception()

for data, executable in zip(inputs, executables):
  execute_command('ant clean && ant')
  output = executable
  execute_command('hadoop fs -rm -f -r temp ' + output)
  execute_command('rm -rf ' + executable)
  execute_command('hadoop jar jar/' + executable + '.jar ru.mipt.' + executable + ' ' + data + ' ' + output)
  execute_command('hadoop fs -get ' + output)
