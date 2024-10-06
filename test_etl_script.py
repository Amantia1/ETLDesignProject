import pytest
from etl import validate_data

def test_validate_data_valid():
    record = {'TransactionNo': 581475, 'date': '12/9/2019', 'ProductNo':23235, 'ProductName':'Bag 500g Swirly Marbles', 'Price':10.65, 'Quantity':24, 'CustomerNo':13069, 'Country':'United Kingdom'}
    try:
        validate_data(record)
    except ValueError:
        pytest.fail("validate_data raised ValueError unexpectedly!")

def test_validate_data_missing_Country():
    record = {'TransactionNo': 581475, 'date': '12/9/2019', 'ProductNo':23235, 'ProductName':'Bag 500g Swirly Marbles', 'Price':10.65, 'Quantity':24, 'CustomerNo':13069}
    with pytest.raises(ValueError, match="Missing required field: Country"):
        validate_data(record)

def test_validate_data_invalid_CustomerNo():
    record = {'TransactionNo': 581475, 'date': '12/9/2019', 'ProductNo':23235, 'ProductName':'Bag 500g Swirly Marbles', 'Price':10.65, 'Quantity':24, 'CustomerNo':-13069,'Country':'United Kingdom'}
    with pytest.raises(ValueError, match="Invalid CustomerNo: must be a positive integer"):
        validate_data(record)



