#---------------- Python 3.7
# data class
from dataclasses import dataclass
@dataclass
class C:
    a: int       # 'a' has no default value
    b: int = 0 

print(f" {C(a=5)=}  {C(a=5).a=}")

#---------------- Python 3.8
# walrus (affectation)
liste_init = ["Python","PEP","conda"]
print(liste_filtree := [y for x in liste_init if "P" in (y:=x.upper())] )

# fstring à la icecream
print(f"{liste_init=}")

#---------------- Python 3.9
# pipe
d1 = {"a": 4, "b": 2, "c": 5}
d2 = {"a": 3, "d": 7, "e": 6}

d_fusion = d1 | d2

# prefix et suffixes
s = "anticonformiste"
print(s.removeprefix("anti"))

# nouveaux types natifs pour les checks
def greet_all(names: list[str]) -> None:
    for name in names:
        print("Hello", name)

"""
#----------------- Python 3.10
# match case simple

http_code == "418"
match http_code:
    case "200":
        print("OK")
    case "404":
        print("Not Found")
    case "500":
        print("Internal Error")
    case _:
        print("Code not found")

# match case sur la structure
dict_a = {
    'id': 1,
    'meta': {
        'source': 'abc',
        'location': 'nord'
    }
}

dict_b = {
    'id': 2,
    'source': 'def',
    'location': 'nord'
}
for d in [dict_a, dict_b, "test"]:
    match d :
        case {'id': ident,
              'meta':{'source': source,
                      'location': loc}}:
            print(ident, source, loc)
        
        case {'id': ident,
              'source': source,
              'location': loc}:
            print(ident, source, loc)
        
        case _:
            print("no match")
"""
