from struct import pack, unpack
from sys import argv

def arr2float(arr):
    return unpack("<f", pack("<HH", *arr))[0]

def float2arr(fl):
    return list(unpack("<HH", pack("<f", fl)))

def auto_decode(x):
    if len(x) == 1:
        return x[0]
    elif len(x) == 2:
        return arr2float(x)
    elif len(x) == 8:
        return x[0]
    else:
        raise ValueError

def auto_encode(x):
    if type(x) is int:
        return [x]
    elif type(x) is float:
        return float2arr(x)
    elif type(x) is bool:
        return [x, False, False, False, False, False, False, False]
    else:
        raise ValueError

def auto(x):
    if type(x) is list:
        return auto_decode(x)
    else:
        return auto_encode(x)

def main():
    print(auto(eval(argv[1])))

if __name__ == "__main__":
    main()
