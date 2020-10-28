from decimal import Decimal


with open('./ETL/__version__.py') as fp:
    exec(fp.read())
    ver = str(__version__)
    major = ver.split('.')[0]
    minor = ver.split('.')[1]
    version = major + '.' + str(sum(map(Decimal, [minor, '1'])))


with open('./ETL/__version__.py', 'w') as fp:
    fp.write("__version__ = '{}'\n".format(version))
