
import re
from decimal import Decimal
def clearCurrencyFormat (number) -> Decimal:
    separators = re.sub(r'\d', '', number, flags=re.U)
    
    for sep in separators[:-1]:
        number = number.replace(sep, '')
        if separators:
            number = number.replace(separators[-1], '.')
    return(number)


# remove all characters not allowed by Zenith

def alnum(input) -> str:
    valids = []
    for character in input:
        if character.isalnum():
            valids.append(character)
    return ''.join(valids)