codigo = "1Ux4#8#17"
  
build = codigo.split('x')
if build[0] == '1U':
    for row in build[1].split('#'):
        print(row)