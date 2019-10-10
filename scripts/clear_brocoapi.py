import os,sys

try:
    os.system('rm -r content/data_tables/*')
    os.system('rm -r content/split_files/*')

except Exception as err:
    print(err)
    sys.exit()